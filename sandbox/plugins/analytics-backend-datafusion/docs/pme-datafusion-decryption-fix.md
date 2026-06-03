# Fix: DataFusion PME Decryption — "encrypted footer but decryption properties were not provided"

## Background

`DataFusionEncryptedQueryExecutionTests` tests three SQL execution scenarios over
PME-encrypted Parquet files:

- `testSelectAllEncrypted` — full table scan
- `testFilterEncrypted` — predicate push-down
- `testAggregationEncrypted` — aggregation over encrypted columns

All three failed with:

```
Parquet error: Parquet file has an encrypted footer but decryption properties were not provided
```

## How DataFusion PME Decryption Works

DataFusion's Parquet support resolves per-file decryption properties through a
two-step mechanism:

1. **Factory registration** — an `EncryptionFactory` is registered in the
   `RuntimeEnv` under a string key called the *factory ID*:

   ```rust
   runtime_env.register_parquet_encryption_factory("my_factory", Arc::new(MyFactory));
   ```

2. **Format wiring** — the `ParquetFormat` used to open files must declare which
   factory to use by setting `options.crypto.factory_id`:

   ```rust
   let mut opts = TableParquetOptions::default();
   opts.crypto.factory_id = Some("my_factory".to_owned());
   let fmt = ParquetFormat::new().with_options(opts);
   ```

When DataFusion opens a Parquet file it calls
`get_file_decryption_properties` on the `ParquetFormat`. The relevant code in
`datafusion-datasource-parquet/src/file_format.rs` is (abridged):

```rust
fn get_file_decryption_properties(...) -> Option<FileDecryptionProperties> {
    let factory_id = self.options().crypto.factory_id.as_deref()?;  // ← returns None if not set
    let factory = runtime.get_parquet_encryption_factory(factory_id)?;
    factory.get_file_decryption_properties(...)
}
```

The `?` on the first line means that if `factory_id` is `None` the function
returns `None` immediately, **regardless of whether a factory is registered**.
Registering a factory in `RuntimeEnv` without also setting `factory_id` on
`ParquetFormat` is therefore a no-op.

## Root Cause

The code had two execution paths that each created a `ParquetFormat`:

### Path 1 — `sql_to_substrait` (schema inference + Substrait plan generation)

`api.rs::sql_to_substrait` registered the factory but used a bare
`ParquetFormat::new()`:

```rust
// factory registered ✓
runtime_env.register_parquet_encryption_factory(FACTORY_ID, Arc::new(factory));

// factory_id NOT set on format ✗
let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))  // ← bug
```

This caused `infer_schema` to fail when reading the encrypted Parquet footer.

### Path 2 — `execute_query` (physical plan execution)

`query_executor.rs::execute_query` had the same pattern — factory registered,
`ParquetFormat` created without `factory_id`:

```rust
// factory registered ✓
runtime_env.register_parquet_encryption_factory(FACTORY_ID, Arc::new(factory));

// factory_id NOT set ✗
let file_format = ParquetFormat::new();  // ← bug
let listing_options = ListingOptions::new(Arc::new(file_format))
```

This caused `executeQueryAsync` to fail when the physical scan opened the file.

## Why This Was Hard to Find

### Symptom appeared in the wrong place

The error "decryption properties were not provided" is thrown deep inside
`parquet-rs` when it tries to read the file footer.  It surfaces as a generic
Parquet error and gives no hint that the root cause is a missing `factory_id`
field on a Rust config struct.

### Factory registration looked correct

The `EncryptionFactory` implementation and its `register_parquet_encryption_factory`
call were correct.  The factory lookup only happens *after* `factory_id` is
checked, so the factory was never reached — it appeared correctly registered but
was silently skipped.

### Two independent code paths

`sql_to_substrait` (called from `NativeBridge.sqlToSubstrait`) and
`execute_query` (called from `NativeBridge.executeQueryAsync`) build their own
`SessionContext` and their own `ParquetFormat`.  Fixing one path left the other
broken.  Debug output showed that `sqlToSubstrait` was successfully calling
`get_file_decryption_properties`, which masked the fact that `executeQueryAsync`
still failed.

### Minimal DataFusion documentation

The `TableParquetOptions::crypto.factory_id` field is not prominently documented.
The connection between "register factory in RuntimeEnv" and "set factory_id on
ParquetFormat" is only visible by reading the DataFusion source.

## Fix

### `api.rs` — `sql_to_substrait`

```rust
let parquet_format = if file_footer_keys.is_empty() == false {
    runtime_env.register_parquet_encryption_factory(
        OPENSEARCH_PME_FACTORY_ID,
        Arc::new(OpenSearchPmeDecryptionFactory::new(
            Arc::clone(&file_footer_keys),
            Arc::clone(&file_aad_prefixes),
        )),
    );
    let mut parquet_options = TableParquetOptions::default();
    parquet_options.crypto.factory_id = Some(OPENSEARCH_PME_FACTORY_ID.to_owned()); // ← fix
    ParquetFormat::new().with_options(parquet_options)
} else {
    ParquetFormat::new()
};
```

### `query_executor.rs` — `execute_query`

```rust
let file_format = if file_footer_keys.is_empty() == false {
    let mut parquet_options = TableParquetOptions::default();
    parquet_options.crypto.factory_id = Some(OPENSEARCH_PME_FACTORY_ID.to_owned()); // ← fix
    ParquetFormat::new().with_options(parquet_options)
} else {
    ParquetFormat::new()
};
let listing_options = ListingOptions::new(Arc::new(file_format))
```

### `DataFusionEncryptedQueryExecutionTests.java` — test schema mismatch

The test wrote the `id` column as Arrow `Int(32, true)` (32-bit integer) but
asserted results with `Long` literals (`assertEquals(1L, ...)`).  Arrow returns
Int32 columns as Java `Integer`, causing:

```
expected: java.lang.Long<2> but was: java.lang.Integer<2>
```

Fix: change the schema to `Int(64, true)` and update `exportRow` to use
`BigIntVector` instead of `IntVector`:

```java
// before
new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
// org.apache.arrow.vector.IntVector idVec = ...

// after
new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)
// org.apache.arrow.vector.BigIntVector idVec = ...
```

## Rule of Thumb

Whenever PME decryption is needed in DataFusion, **both** of these must be done
together — one without the other silently skips decryption:

```rust
// 1. Register the factory
runtime_env.register_parquet_encryption_factory(FACTORY_ID, Arc::new(my_factory));

// 2. Wire the factory to ParquetFormat
let mut opts = TableParquetOptions::default();
opts.crypto.factory_id = Some(FACTORY_ID.to_owned());
let format = ParquetFormat::new().with_options(opts);
```

