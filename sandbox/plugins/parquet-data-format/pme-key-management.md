# Parquet PME Key Management

This document defines the target key-management model for Parquet Modular
Encryption (PME) in the `parquet-data-format` sandbox plugin.

The v1 design intentionally mirrors the current Lucene storage-encryption model:

```text
OpenSearch metadata:
  index-level keyfile -> provider-wrapped data key bytes

Parquet file metadata:
  data_key_id = "default"
  message_id

Read:
  use data_key_id = "default" to select the index-level keyfile
  unwrap the data key once
  derive each file key locally from data key + message_id
```

There is no key rotation in v1. The only valid `data_key_id` is `"default"`.
This is redundant today, but it gives the footer a stable way to say which data
key it expects without introducing a separate rotation design. PME metadata
`version: 1` implies the only derivation algorithm.

## Why This Shape

A per-file wrapped key would make each Parquet file self-contained, but cold
reads could require one KMS decrypt per Parquet file. Because Parquet files are
segment-like, a shard can contain many of them.

The simpler v1 model avoids that:

- KMS is called once for the data key per cache miss.
- Every Parquet file carries `data_key_id: "default"` and its per-file
  `message_id`.
- Per-file keys are derived locally.
- The design is close to the Lucene plugin and has the same no-rotation
  assumption.

## Durable Metadata

### PME Footer Key Metadata

This metadata is stored in Parquet PME `FileCryptoMetaData.key_metadata`, exposed
by parquet-rs through `FileEncryptionProperties::with_footer_key_metadata(...)`.

V1 payload is UTF-8 JSON:

```json
{
  "version": 1,
  "data_key_id": "default",
  "message_id": "base64url-16-random-bytes"
}
```

That is the complete v1 file metadata contract. Writers should emit compact JSON
with exactly these fields, in the order shown, and no insignificant whitespace.
Readers should parse it as JSON, not by string matching, and reject missing
fields, malformed values, unknown versions, and unexpected fields.

The exact bytes stored in `FileCryptoMetaData.key_metadata` are:

```text
UTF8(compact_json({
  "version": 1,
  "data_key_id": "default",
  "message_id": base64url_without_padding(message_id[16])
}))
```

Example:

```json
{"version":1,"data_key_id":"default","message_id":"VfN2M7dPjJxYpI3aLc0S6A"}
```

Field meanings:

`version`
: Metadata version. For `version: 1`, the derivation algorithm is fixed by this
  document.

`data_key_id`
: Selects the index-level OpenSearch data key. In v1 this must be `"default"`,
  which resolves to the same index-level `keyfile` concept used by Lucene
  storage encryption.

`message_id`
: Random 16-byte value generated when the Parquet file is written. This is the
  per-file input to key derivation, analogous to Lucene storage encryption's file
  footer `messageId`. In JSON it is encoded as unpadded base64url, which is 22
  characters for a 16-byte value.

### OpenSearch Data Key Metadata Location

The wrapped data key is stored by OpenSearch, not inside every Parquet file. V1
follows the Lucene storage-encryption plugin's location and payload shape.

Lucene storage encryption stores its wrapped data key in an index-level
`keyfile`, under:

```text
.../indices/{index-uuid}/keyfile
```

Parquet PME v1 should use the same index-level `keyfile` convention. The file
payload is the encrypted data key bytes returned by
`MasterKeyProvider.generateDataPair().getEncryptedKey()`. It is not a Parquet
JSON document and it is not a `data_keys` map.

Motivation:

- matches Lucene's operational model
- gives one wrapped data key per index in v1
- avoids one KMS decrypt per Parquet file
- avoids copying key material metadata into every shard directory
- keeps shard relocation/recovery focused on data files while the index-level
  `keyfile` remains the shared source of truth

The Parquet file references this key through `data_key_id`. In v1 that value is
always `"default"`, which means:

```text
data_key_id = "default"
  -> .../indices/{index-uuid}/keyfile
  -> keyProvider.decryptKey(keyfile bytes)
  -> dataKey
```

### Provider and KMS Metadata

Provider and KMS metadata follow Lucene storage encryption. V1 does not define a
separate Parquet provider/KMS metadata schema.

The key provider, KMS key identifier, and encryption context are resolved through
the same OpenSearch/Lucene storage-encryption provider model that supplies
`MasterKeyProvider`. The `keyfile` stores only provider-wrapped data key bytes;
provider selection and provider settings are not duplicated in the PME footer.

#### Parquet index settings vs. Lucene index settings

The Lucene storage-encryption plugin registers a single index setting:

```
index.store.crypto.key_provider   →  provider name only
```

The provider *type* is resolved out-of-band from the `CryptoHandlerRegistry` by
looking up the registered `CryptoKeyProviderPlugin` for that name.

Parquet PME uses a separate, Parquet-specific prefix to avoid a setting-name
collision (two plugins cannot register an `IndexScope` setting under the same
key):

```
index.store.parquet.crypto.key_provider       →  provider name
index.store.parquet.crypto.key_provider_type  →  provider type (e.g. "mock-pme")
```

The `key_provider_type` field was added so that the correct
`CryptoKeyProviderPlugin` can be selected directly from the index settings
without an additional registry lookup.

Index setting semantics are intentionally out of scope for this document.

## V1 Derivation Algorithm

For PME metadata `version: 1`, derive the final Parquet footer key exactly as
follows:

```text
dataKey = keyProvider.decryptKey(readBytes(indexLevelKeyfile))
messageId = base64url_decode(fileMetadata.message_id)
context = UTF8("opensearch/parquet-pme/footer-key/v1")

PRK = HMAC-SHA384(key = dataKey, data = messageId)
T1 = HMAC-SHA384(key = PRK, data = context || 0x01)
pmeFooterKey = first 32 bytes of T1
```

### Structure

The derivation has two HMAC steps because it follows the same extract/expand
shape used by the Lucene storage-encryption implementation.

The intent is to reuse the Lucene pattern rather than introduce a second
OpenSearch-local key derivation style. Lucene derives file keys as:

```text
PRK = HMAC-SHA384(key = masterKey, data = messageId)
fileKey = first 32 bytes of HMAC-SHA384(key = PRK, data = "file-encryption" || 0x01)
```

Parquet uses the same shape, but with the Parquet data key and a
Parquet-specific context string.

Step 1 mixes the secret data key with the per-file `message_id`:

```text
PRK = HMAC-SHA384(key = dataKey, data = messageId)
```

This produces an intermediate pseudorandom key that is unique to this file. The
same `dataKey` can serve many Parquet files because each file has a different
random `message_id`.

Step 2 turns that intermediate key into a key for one specific purpose:

```text
T1 = HMAC-SHA384(key = PRK, data = context || 0x01)
```

The `context` string says what this derived key is for. Here it is specifically
for OpenSearch Parquet PME footer/page encryption. The `0x01` byte is the first
HKDF-style expansion counter. We only need one block because HMAC-SHA384 outputs
48 bytes and the PME key uses the first 32 bytes.

The result is:

```text
pmeFooterKey = first 32 bytes of T1
```

### Why Use A Parquet Context String

The context is:

```text
opensearch/parquet-pme/footer-key/v1
```

Lucene uses the same derivation shape with a different context:

```text
file-encryption
```

This means that even if the same data key and the same `message_id` were ever
used in both systems by mistake, the derived keys would still be different. The
context makes the derived key specific to Parquet PME rather than a generic file
encryption key.

### Validation

The validation checks are not extra cryptographic features. They are guardrails
that ensure the implementation is really following the v1 contract:

- `data_key_id` is present and equals `"default"` in v1
- `dataKey.length == 32`, because v1 derives AES-256 keys only
- `messageId.length == 16`, because the derivation expects a fixed-size per-file
  value
- output key length is always 32 bytes
- unsupported metadata versions fail closed

## Write Flow

1. Resolve the index-level PME/Lucene-style keyfile.

2. If the index-level `keyfile` does not exist, create it:

   - call `MasterKeyProvider.generateDataPair()`
   - persist `encryptedKey` as the raw `keyfile` bytes
   - keep `rawKey` only long enough to derive keys or hydrate the cache

3. For each new Parquet file:

   - generate a random 16-byte `message_id`
   - derive `pmeFooterKey` from the unwrapped index data key and `message_id`
   - write PME footer key metadata containing only:
     - `version`
     - `data_key_id`
     - `message_id`
   - pass `pmeFooterKey` to parquet-rs
   - best-effort zero temporary key material

4. OpenSearch commit metadata may store a summary for planning/debugging, but the
   v1 bootstrap data is the file `data_key_id` and `message_id` plus the
   index-level `keyfile`.

## Read Flow

This section describes the logical hydration/decryption steps only. It does not
define a reader bootstrap API.

1. Read PME `FileCryptoMetaData.key_metadata` before decrypting the Parquet
   footer.

2. Parse and validate the v1 metadata:

   - supported `version`
   - valid `data_key_id`
   - valid 16-byte `message_id`

3. Resolve `data_key_id = "default"` to the index-level `keyfile`.

4. Validate provider policy:

   - the provider is the one resolved by the Lucene-compatible OpenSearch
     encryption configuration
   - provider/KMS failures are surfaced as key hydration failures

5. Hydrate the data key:

   ```java
   byte[] dataKey = keyProvider.decryptKey(keyfileBytes);
   ```

6. Derive the file key using the v1 algorithm.

7. Pass the derived `pmeFooterKey` to the native Rust/DataFusion layer
   (`FileDecryptionProperties`). Java does not hold a `resolveFooterKey` API
   in `PmeContext` — key material for the read path is supplied to DataFusion
   when the reader is initialised.

8. Best-effort zero temporary key material, or release it through a scoped cache.

## Key Cache

KMS decrypts are cached with shard-level granularity, mirroring the Lucene
storage-encryption plugin's `NodeLevelKeyCache` / `ShardCacheKey` design:

```text
cache key  (PmeCacheKey):
  index UUID
  shard ID

cache value:
  hydrated dataKey (PmeDataKey)
```

### Why shard-level, not index-level

Using `(indexUuid, shardId)` as the cache key gives precise lifecycle control:
each shard's entry is evicted independently when the shard's engine closes, with
no need to reference-count how many shards of the same index are still open on
the node. This is the same trade-off the Lucene plugin makes. Multiple shards of
the same index on the same node each decrypt the shared index-level keyfile once
on cold cache miss and hold a separately cached copy of the same decrypted data
key — an acceptable memory overhead for the lifecycle simplicity it provides.

### Lifecycle

- hydrate on shard open (cache miss → KMS decrypt of index-level keyfile)
- evict on shard engine close — `PmeContext.evict()` zeros key material
- `PmeDataKeyCache.reset()` clears all entries (test / node shutdown use)
- best-effort zero on eviction via `PmeDataKey.zero()`

## No Rotation In V1

V1 has no key rotation. The only valid `data_key_id` is `"default"`. If rotation
is added later, a newer metadata version can define additional key identifiers
and their durable storage rules.

Until then, the default data key must remain available for as long as any encrypted
Parquet file in the index exists.

## Merge

Merge should decrypt inputs and write a new output file:

1. Read each input file using `data_key_id` and its own `message_id`.
2. Decrypt through the normal Parquet reader.
3. Write the merged output with `data_key_id = "default"`.
4. Generate a new `message_id` for the output file.
5. Delete old inputs only after commit succeeds.

## Snapshot and Restore

Snapshots must preserve both:

- Parquet files with PME footer key metadata
- the index-level OpenSearch `keyfile`

Restore policy must decide whether KMS/provider references are valid in the
target cluster. If the referenced provider or KMS key cannot be used, restore must
fail or perform an explicit rewrap/re-encryption step.

## AAD

AAD v1 is concrete and intentionally small:

```text
domain = UTF8("opensearch/parquet-pme/file/v1")
version = 1
dataKeyId = UTF8(fileMetadata.data_key_id)
messageId = base64url_decode(fileMetadata.message_id)

aad_prefix =
  u16_be(domain.length)
  domain
  u8(version)
  u16_be(dataKeyId.length)
  dataKeyId
  messageId[16]
```

For v1, this means:

```text
u16_be(30)
UTF8("opensearch/parquet-pme/file/v1")
u8(1)
u16_be(7)
UTF8("default")
16 raw message_id bytes
```

The resulting bytes are passed to Parquet as the PME AAD prefix.

The AAD uses `message_id` rather than file name. The `message_id` is already the
file-local derivation input, is fixed-size, and is persisted in PME key metadata.
Using the file name would couple decryption to naming and rename/copy behavior in
refresh, recovery, merge, and snapshot flows. That coupling is not needed for v1.

Do not include:

- final logical file name
- absolute filesystem path
- node ID
- allocation ID
- wall-clock time

The AAD prefix is binary, not JSON. Do not rely on map iteration order, JSON
serialization details, or ad hoc string concatenation.

## Open Issues

### AES-128-GCM constraint from parquet-rs 57.x

The v1 derivation algorithm in this document specifies a 32-byte (`pmeFooterKey`)
output, implying AES-256-GCM. However, parquet-rs 57.x only supports
**AES-128-GCM** (16-byte keys) in its encryption cipher implementation
(`encryption/ciphers.rs` uses `ring::aead::AES_128_GCM` exclusively). Passing a
32-byte key to `ring` returns `Unspecified`, causing every encrypted write to
fail at runtime.

As a pragmatic fix, `FOOTER_KEY_BYTES` was reduced from 32 to **16** and the
Rust-side validation was updated to match. The derivation algorithm still runs
two HMAC-SHA384 steps; it simply truncates `T1` to 16 bytes instead of 32:

```text
pmeFooterKey = first 16 bytes of T1
```

The Rust guard `footer_key.len() != 16` enforces this at the FFM boundary.

**Impact on this document:**

- Every reference to "32-byte footer key" or "AES-256" in the derivation section
  is factually incorrect for the current implementation. The derivation algorithm
  box should read `pmeFooterKey = first 16 bytes of T1`.
- `PmeKeyDerivation.FOOTER_KEY_BYTES = 16` is the authoritative constant.
- The data key (index-level keyfile) remains 32 bytes; only the **derived**
  per-file footer key is 16 bytes.
- `validateKeyLength` in `PmeKeyfileManager` still checks for
  `DATA_KEY_BYTES = 32`; the footer-key length check lives in Rust.

**Planned resolution:** upgrade to a version of parquet-rs (Apache Arrow/Parquet
≥ 58.x or a fork) that exposes AES-256-GCM support, restore `FOOTER_KEY_BYTES`
to 32, update the Rust guard, and revert the truncation. Until then AES-128-GCM
is the operative algorithm.

### Setting-name collision between Lucene and Parquet crypto plugins

The opensearch-storage-encryption (Lucene) plugin registers
`index.store.crypto.key_provider` as an `IndexScope + NodeScope + InternalIndex`
setting. OpenSearch forbids two plugins from registering an `IndexScope` setting
under the same key, so the Parquet plugin uses its own namespace:

```
index.store.parquet.crypto.key_provider
index.store.parquet.crypto.key_provider_type
```

This is a temporary workaround. The goal is a unified solution where both the
Lucene and Parquet plugins share a single `index.store.crypto.key_provider`
setting, either through:

- a core-registered prefix setting that both plugins can read without
  re-registering, or
- a merged crypto-metadata registry in the server that exposes the provider
  name/type to any plugin that needs it.

Until that mechanism exists, the Parquet-specific prefix avoids the collision at
the cost of a separate (but semantically identical) index setting key.

## Security Notes

- PME file metadata is plaintext bootstrap data. Treat it as untrusted until the
  file decrypts and AAD validation succeeds.
- Do not store plaintext data keys or derived file keys in durable metadata.
- Keep KMS encryption context short and non-sensitive.
- Do not let file metadata select arbitrary providers outside index/cluster
  policy.
- Fail closed for unknown versions, missing data key metadata, malformed file
  metadata, or derivation mismatches.

## Implementation (v1 — implemented)

### Java (`org.opensearch.parquet.encryption` package)

| Class | Role |
|---|---|
| `PmeFileKeyMetadata` | v1 JSON serializer/parser for `FileCryptoMetaData.key_metadata` |
| `PmeKeyDerivation` | Two-step HMAC-SHA384 key derivation and binary AAD prefix builder |
| `PmeDataKey` | 32-byte key holder with best-effort `zero()` on eviction |
| `PmeCacheKey` | Immutable composite cache key `(indexUuid, shardId)`;  Mirrors `ShardCacheKey` from the Lucene storage-encryption plugin. |
| `PmeDataKeyCache` | Node-level singleton cache keyed by `PmeCacheKey`; `initialize()` called from `ParquetDataFormatPlugin.createComponents()`; shard-level eviction on engine close; `reset()` for test teardown. |
| `PmeKeyfileManager` | Atomic `CREATE_NEW` index-level keyfile creation/read; multiple shards race, loser reads winner's file |
| `PmeFileEncryptionInputs` | Per-file bundle of derived footer key + key metadata JSON + AAD prefix; `zero()` called after native writer init |
| `PmeContext` | Per-engine facade; `create(IndexSettings, Path indexDataPath, int shardId)` → pre-warms shard cache entry; `createFileEncryptionInputs()` on write path; `evict()` on engine close (zeroes this shard's entry only). Read-path key resolution runs through the native Rust/DataFusion layer, not through `PmeContext`. |

Key decisions:

- `MasterKeyProvider` is opened via try-with-resources per cache miss (KMS call),
  not held open for the engine lifetime.
- Index-level keyfile is at `<index-data-dir>/keyfile` (two levels above
  `shardPath.getDataPath()`).
- `ParquetModularEncryptionConfig` is deprecated and no longer used.

### Rust (`parquet_create_writer` simplified signature)

`parquet_create_writer(file, schema_address, footer_key, key_metadata_json, aad_prefix)`

- `footer_key`: 32-byte derived key from Java (null ⇒ unencrypted)
- `key_metadata_json`: UTF-8 JSON stored via `with_footer_key_metadata(...)`
- `aad_prefix`: binary prefix configured via `with_aad_prefix(...)`

Read functions (`parquet_get_file_metadata_decrypted`,
`parquet_get_decrypted_num_rows`) accept the same `footer_key` + `aad_prefix`.

`parquet_read_key_metadata(file, out_buf, out_buf_len, out_len_out)` reads
`FileCryptoMetaData.key_metadata` without decryption using a hand-rolled
Thrift compact protocol parser. Returns 1 if the file is not encrypted.

### Tests

Implemented unit tests (all passing):

| Test class | Coverage |
|---|---|
| `PmeKeyfileManagerTests` | keyfile creation, load, concurrent-init race, wrong-length rejection |
| `PmeDataKeyCacheTests` | cache miss/hit, zero-on-evict, shard isolation, reset |
| `PmeContextTests` | create returns null for plain/null settings, cache pre-warm, evict zeros key, evict+re-create |
| `PmeFileKeyMetadataTests` | `forNewFile` validation + defensive copy, `toJsonBytes` compact form, `parse` round-trip + all rejection cases (unknown version, wrong data_key_id, missing fields, unknown fields, malformed base64, wrong message_id length) |
| `PmeKeyDerivationTests` | `deriveFooterKey` null/length guards, determinism, sensitivity to data key and message_id, known all-zero vector; `buildAadPrefix` null/length guards, byte-level structure, determinism, sensitivity to message_id |

Pending integration / scenario tests:

- Restart reads old encrypted files.
- Many files in one index require one KMS unwrap per cold data-key cache.
- Missing index-level `keyfile` fails.
- Provider/KMS failures follow the Lucene-compatible key-provider path.
