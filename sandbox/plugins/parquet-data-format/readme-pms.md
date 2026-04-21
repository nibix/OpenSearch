# Parquet PME Prototype (PMS Notes)

This document describes the current prototype integration of Parquet Modular Encryption (PME) in the `parquet-data-format` sandbox plugin, and what is needed to make it production-ready.

> Naming note: this file is intentionally named `readme-pms.md` per request. The implementation itself consistently uses the term PME.

## Scope and status

Implemented prototype scope:

- Uses OpenSearch crypto plumbing (`CryptoHandlerRegistry`) to validate KMS availability.
- Obtains file encryption key material via key provider (`MasterKeyProvider.generateDataPair()`).
- Passes key material through Java -> FFM -> Rust.
- Enables real Parquet encryption in Rust via `FileEncryptionProperties`.
- Supports prototype decrypt reads for encrypted files (footer metadata + payload row iteration) when the decrypted footer key is provided.
- Stores KMS metadata markers in Parquet key-value metadata for traceability.

Scope decision for this prototype track:

- No external interoperability is required.
- Only OpenSearch is expected to read these encrypted Parquet files.
- The initial production path can stay footer-driven (no mandatory separate catalog envelope).

Current validation status (local):

- Rust library tests: `cargo test -q --lib` pass.
- Java targeted test (with sandbox enabled + JDK 25):
  - `:sandbox:plugins:parquet-data-format:test --tests "org.opensearch.parquet.bridge.ParquetModularEncryptionConfigTests"` pass.

## Architecture (current)

### 1) Java engine bootstrap

In `ParquetIndexingEngine.initializeEncryption(...)`:

1. Reads index crypto settings via `CryptoMetadata.fromIndexSettings(...)`.
2. Uses `CryptoHandlerRegistry.getInstance()` and `fetchCryptoHandler(...)` for fail-fast validation.
3. Resolves the `CryptoKeyProviderPlugin` from registry.
4. Creates a `MasterKeyProvider` and calls `generateDataPair()`.
5. Builds `ParquetModularEncryptionConfig` with:
   - `footerKey` = raw data key bytes
   - `wrappedFooterKey` = encrypted data key bytes
   - provider identity/context fields

### 2) Java bridge and FFM contract

`RustBridge.createWriter(...)` now sends an all-or-nothing encryption payload:

- `kmsInstanceId`
- `kmsInstanceType`
- `kmsKeyArn`
- `kmsEncryptionContext`
- `footerKey` (raw key)
- `wrappedFooterKey` (encrypted key)

In `ffm.rs`, `parquet_create_writer(...)` validates the payload atomically:

- All encryption fields present -> PME path enabled.
- All fields absent -> plaintext path.
- Mixed/partial fields -> explicit error.

### 3) Rust writer

In `writer.rs`:

- `ParquetEncryptionOptions` contains both raw and wrapped footer key.
- On encrypted path, writer builds:
  - `FileEncryptionProperties::builder(options.footer_key.clone())`
  - `.with_footer_key_metadata(options.wrapped_footer_key.clone())`
- Applies encryption properties to Parquet writer properties.
- Adds prototype metadata markers, including wrapped key length.

### 4) Prototype decrypt read (metadata + payload rows)

In `writer.rs` + `ffm.rs` + `RustBridge`:

- `NativeParquetWriter::get_file_metadata_decrypted(...)` reads encrypted footer metadata using
  `FileDecryptionProperties::builder(footer_key)`.
- `NativeParquetWriter::get_decrypted_num_rows(...)` iterates decrypted record batches and returns
  the decrypted payload row count.
- New FFM export `parquet_get_file_metadata_decrypted(...)` exposes this path to Java.
- New FFM export `parquet_get_decrypted_num_rows(...)` exposes payload decrypt reads to Java.
- Java can call `RustBridge.getFileMetadata(file, encryptionConfig)` and
  `RustBridge.getDecryptedNumRows(file, encryptionConfig)` for encrypted files.

Current limitation:

- This is currently a bridge-level payload decrypt API (stats/iteration) and is not yet wired into
  the full query/execution reader path.

Clarification:

- "Bridge-level" means decrypt reads are currently available through explicit bridge calls.
- The normal query planner/executor path does not yet automatically use this decrypt reader path.

## Key derivation and key handling

This is the most important part.

## What happens today

### Source of key material

The prototype does **not** locally derive a DEK from a password or static secret in the plugin.

Instead:

- Java asks the configured KMS provider through OpenSearch crypto interfaces.
- `MasterKeyProvider.generateDataPair()` returns a `DataKeyPair`:
  - `rawKey`: plaintext data key bytes (used directly for Parquet encryption)
  - `encryptedKey`: wrapped form of that same data key (stored as Parquet footer key metadata)

So the effective derivation/generation strategy is delegated to the provider implementation (for example AWS KMS provider internals).

### Consequences

- Good: no custom KDF logic in plugin code, less crypto footguns.
- Good: key provenance and wrapping policy are centralized in provider/KMS.
- Gap: key lifecycle hardening in process memory is still minimal in this prototype.

## What must be clarified for production

1. **Authoritative key semantics contract**
   - Confirm provider contract guarantees for `generateDataPair()`:
     - entropy source
     - key size options (AES-128/192/256)
     - wrapping algorithm and metadata format
   - Document that contract as API assumptions for PME.

2. **Key size policy**
   - Enforce accepted lengths (16/24/32) at Java boundary before crossing FFM.
   - Reject unsupported keys with explicit errors.

3. **AAD strategy**
   - Define deterministic AAD prefix (cluster/index/shard/file identity).
   - Ensure anti-swap guarantees across restore/move scenarios.

4. **Footer metadata contract (OpenSearch-only)**
   - Define a strict, versioned footer contract for wrapped-key metadata and reader policy fields.
   - Include provider id/type, key id/arn reference, context hash, and contract version.
   - Treat missing or mismatched required fields as fail-closed errors.

Note on metadata placement:

- A separate PME envelope in catalog metadata is optional and can be added later if needed.
- For the current direction, footer metadata is the primary contract surface.

## Security boundaries and current risks

Current prototype risks to address:

- Plaintext `rawKey` currently lives in Java byte arrays and Rust `Vec<u8>`.
- No explicit zeroization/wiping on all error/close paths.
- Prototype metadata currently stores `kms_encryption_context` as plain text; if this context contains user, tenant, or business details, those details can leak via Parquet metadata.
  - Do: keep context minimal and non-sensitive (for example opaque IDs only).
  - Don't: include emails, customer names, ticket IDs, or free-form business data.
- Full reader-side decryption flow for row data is not yet wired into the plugin query/execution path.

## Production roadmap

## Phase 1: Crypto hardening (must-have)

- Add strict key-length and payload validation in Java and Rust.
- Zeroize plaintext key buffers after writer init/finalize/error.
- Ensure wrapped key metadata is versioned and bounded in size.
- Add negative tests for malformed metadata and invalid key lengths.

Exit criteria:

- No plaintext-key lingering in normal teardown paths (best-effort + tested).
- Full validation test matrix for encryption payload and key lengths.

## Phase 2: Reader path and restart stability

- Implement decrypt/read path using wrapped footer key metadata.
- Rehydrate key via provider/KMS, not local secrets.
- Validate deterministic read behavior across refresh/restart cycles.

Exit criteria:

- End-to-end encrypted write/read integration tests pass under sandbox.
- Backward-compatible footer contract parsing with versioning.

## Phase 3: Operations and rotation

- Define key rotation behavior (new files use new keys; old files remain readable).
- Add telemetry/audit hooks for key generation and unwrap events.
- Document failure modes and fallback policy (fail-closed recommended).

Exit criteria:

- Rotation and recovery playbooks tested.
- Metrics and audit coverage available for security operations.

## Phase 4: Performance and reliability

- Benchmark overhead of PME on write path.
- Add concurrency and stress tests for VSR rotation + encrypted writer lifecycle.
- Validate no regressions in checksum/flush/sync behavior.

Exit criteria:

- Performance budget agreed.
- Stability tests pass under repeated randomized runs.

## Practical next steps

1. Add explicit key length guardrails in `ParquetModularEncryptionConfig` and FFM validation (including wrapped metadata bounds).
2. Add zeroization utility for key buffers on both Java and Rust sides.
3. Introduce a strict, versioned footer metadata contract and tests.
4. Add integration tests for encrypted read path once decrypt side is wired.

## Key files

- `sandbox/plugins/parquet-data-format/src/main/java/org/opensearch/parquet/engine/ParquetIndexingEngine.java`
- `sandbox/plugins/parquet-data-format/src/main/java/org/opensearch/parquet/bridge/ParquetModularEncryptionConfig.java`
- `sandbox/plugins/parquet-data-format/src/main/java/org/opensearch/parquet/bridge/RustBridge.java`
- `sandbox/plugins/parquet-data-format/src/main/rust/src/ffm.rs`
- `sandbox/plugins/parquet-data-format/src/main/rust/src/writer.rs`
- `sandbox/plugins/parquet-data-format/src/main/rust/Cargo.toml`

