# Parquet PME vs. Lucene Storage Encryption Review

This document compares the Parquet Modular Encryption prototype in
`sandbox/plugins/parquet-data-format` with the Lucene file encryption implementation in
`opensearch-storage-encryption`. It focuses on architecture, current implementation behavior,
similarities, important deviations, and production hardening notes.

## Source Basis

Parquet PME:

- `sandbox/plugins/parquet-data-format/readme-pms.md`
- `sandbox/plugins/parquet-data-format/src/main/java/org/opensearch/parquet/engine/ParquetIndexingEngine.java`
- `sandbox/plugins/parquet-data-format/src/main/java/org/opensearch/parquet/bridge/ParquetModularEncryptionConfig.java`
- `sandbox/plugins/parquet-data-format/src/main/java/org/opensearch/parquet/bridge/RustBridge.java`
- `sandbox/plugins/parquet-data-format/src/main/java/org/opensearch/parquet/writer/ParquetWriter.java`
- `sandbox/plugins/parquet-data-format/src/main/java/org/opensearch/parquet/vsr/VSRManager.java`
- `sandbox/plugins/parquet-data-format/src/main/rust/src/ffm.rs`
- `sandbox/plugins/parquet-data-format/src/main/rust/src/writer.rs`

Lucene storage encryption:

- `opensearch-storage-encryption/README.md`
- `opensearch-storage-encryption/DEVELOPER_GUIDE.md`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/CryptoDirectoryPlugin.java`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/CryptoDirectoryFactory.java`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/key/DefaultKeyResolver.java`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/key/NodeLevelKeyCache.java`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/key/ShardKeyResolverRegistry.java`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/footer/EncryptionFooter.java`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/niofs/CryptoNIOFSDirectory.java`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/bufferpoolfs/BufferPoolDirectory.java`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/bufferpoolfs/BufferIOWithCaching.java`
- `opensearch-storage-encryption/src/main/java/org/opensearch/index/store/block_loader/CryptoDirectIOBlockLoader.java`

## Executive Summary

The two implementations share the same broad envelope-encryption intent: OpenSearch index settings
identify a key provider, the provider produces or unwraps data key material, and each encrypted file
carries enough metadata for later reads to find the correct keying context.

The architectural center of gravity is very different. Lucene storage encryption is a transparent
Lucene `Directory` implementation. It sits beneath normal indexing and search code, encrypts/decrypts
`IndexOutput` and `IndexInput`, owns key caching, and has a recovery story built around an index-level
`keyfile`. The Parquet prototype is a file-format integration. It injects a PME config into the
Parquet writer path, crosses Java -> FFM -> Rust, and delegates actual Parquet encryption to the
Apache Parquet Rust implementation.

The most important Parquet gaps are reader integration, restart/key rehydration, key granularity,
key validation/zeroization, and a strict metadata contract. The prototype writes encrypted Parquet
files and can decrypt them through bridge-level helper APIs when the raw footer key is already
available, but the normal query/execution reader path does not yet unwrap the stored key metadata or
automatically use the decrypt reader.

## Similarities

- Both use the OpenSearch crypto/KMS abstractions rather than implementing provider-specific KMS calls
  in the encryption layer.
- Both rely on envelope encryption concepts: a plaintext data key is used locally, while a wrapped
  version is persisted for later recovery.
- Both are activated from index-level crypto settings.
- Both are file scoped at the data plane. Lucene encrypts individual Lucene files; Parquet encrypts
  individual Parquet writer outputs.
- Both fail closed for wrong plaintext key material in the tested paths.
- Both persist encryption metadata with the file or index storage so reads can be independent of
  process memory once a rehydration path exists.
- Both keep higher-level indexing APIs mostly unaware of encryption after the encrypted writer/input
  is created.

## Architectural Deviations

### 1. Integration Layer

Lucene storage encryption plugs into OpenSearch as an `IndexStorePlugin` and registers the `cryptofs`
store type through `CryptoDirectoryPlugin.getDirectoryFactories()`. OpenSearch creates encrypted
directories through `CryptoDirectoryFactory`, and Lucene sees normal `Directory`, `IndexInput`, and
`IndexOutput` abstractions.

Parquet PME plugs into the data-format execution path. `ParquetDataFormatPlugin.indexingEngine()`
creates a `ParquetIndexingEngine`; that engine creates `ParquetWriter` instances; and the writer
pushes Arrow batches into native Rust Parquet writer code through `RustBridge`.

Impact:

- Lucene encryption is transparent to the Lucene engine and most OpenSearch read/write flows.
- Parquet encryption must be wired into every Parquet-specific writer and reader path.
- Lucene has a mature place to intercept file operations; Parquet has better semantic alignment with
  Parquet files but more integration surfaces to complete.

### 2. Key Granularity

Lucene storage encryption creates an index-level wrapped data key in `keyfile`, then derives
per-file keys with HKDF using a random per-file `messageId` from `EncryptionFooter`.

Parquet PME currently creates one `ParquetModularEncryptionConfig` in
`ParquetIndexingEngine.initializeEncryption(...)`. That config is stored on the engine and passed to
every `ParquetWriter` created by that engine. The raw key in the config becomes the Parquet footer key
passed to Rust.

Impact:

- Lucene gets per-file key separation even though the wrapped root data key is index-level.
- Parquet appears to reuse the same footer key across all writer generations for the engine lifetime.
- If production expects per-file data keys, Parquet key generation should move from engine bootstrap
  to writer/file creation, or explicitly document engine/shard-scoped key reuse and its rotation model.

### 3. Key Persistence and Rehydration

Lucene storage encryption persists the wrapped data key in `keyfile`. On read, `DefaultKeyResolver`
loads that file, asks the `MasterKeyProvider` to decrypt it, and `NodeLevelKeyCache` caches the result.
Resize and clone flows copy the keyfile so copied encrypted files remain readable.

Parquet PME persists wrapped key material in two places today:

- Rust passes `wrapped_footer_key` to Parquet as footer key metadata.
- Java stores `opensearch.pme.wrapped_footer_key_b64` in `WriterFileSet` metadata.

The read path does not yet use either persisted wrapped key to call `MasterKeyProvider.decryptKey(...)`.
Bridge-level reads require a `ParquetModularEncryptionConfig` that already contains the raw footer key.

Impact:

- Lucene can restart and recover keys from persistent storage.
- Parquet can produce encrypted files but cannot yet independently recover old file keys after restart
  or key rotation.
- Parquet needs a reader-side metadata parser and KMS unwrap path before encrypted files are production
  durable across process restarts.

### 4. Settings Semantics

Lucene storage encryption treats `index.store.crypto.key_provider` as the provider type used to fetch
the `CryptoKeyProviderPlugin`. It has explicit test support for `dummy`.

Parquet PME uses `CryptoMetadata.fromIndexSettings(...)`, which treats
`index.store.crypto.key_provider` as the provider name and defaults
`index.store.crypto.key_provider_type` to `aws-kms`.

Impact:

- Settings that work for `cryptofs` with `key_provider: dummy` can map to provider name `dummy` and
  provider type `aws-kms` in Parquet unless `index.store.crypto.key_provider_type` is also set.
- This should be clarified and tested. Either align Parquet with storage encryption settings or make
  the expected `CryptoMetadata` convention explicit in Parquet docs and validation.

### 5. Crypto Implementation Ownership

Lucene storage encryption owns its file encryption format:

- Custom OSEF trailer/footer.
- AES-256-GCM write path with stored frame tags.
- AES-CTR-compatible random access read path.
- HKDF file-key and IV derivation.
- Direct I/O, block cache, read-ahead, and file-channel caching.

Parquet PME delegates file encryption to Apache Parquet Rust:

- `FileEncryptionProperties::builder(footer_key)`.
- `.with_footer_key_metadata(wrapped_footer_key)`.
- `FileDecryptionProperties::builder(footer_key)` for prototype reads.

Impact:

- Parquet avoids inventing a new file encryption format.
- Lucene has more control over random access, caching, and low-level performance.
- Parquet must track the exact semantics and limitations of the upstream Parquet encryption library.
- Lucene's current read path authenticates the custom footer, but the NIO and Direct I/O read paths
  decrypt payload bytes with CTR-compatible random access and do not appear to verify stored per-frame
  GCM tags during normal reads. Parquet should not inherit that tradeoff accidentally; it should rely
  on the Parquet library's authenticated read semantics and add corruption/tamper tests.

### 6. Reader Path Coverage

Lucene storage encryption has integrated read paths:

- `CryptoNIOFSDirectory.openInput(...)` returns `CryptoBufferedIndexInput`.
- `BufferPoolDirectory.openInput(...)` returns `CachedMemorySegmentIndexInput`.
- `CryptoDirectIOBlockLoader` loads encrypted blocks and decrypts them for cache-backed reads.

Parquet PME has bridge-level reads:

- `RustBridge.getFileMetadata(file, encryptionConfig)`.
- `RustBridge.getDecryptedNumRows(file, encryptionConfig)`.
- Rust loads encrypted metadata and iterates decrypted row batches.

Impact:

- Lucene is wired into normal OpenSearch read/search behavior.
- Parquet read support is currently a proof that decryption works, not a complete query path.
- The next production milestone is to connect decrypted Parquet readers to the actual execution path.

### 7. Metadata Contract

Lucene storage encryption has a purpose-built footer format with magic bytes, frame count, frame size,
message id, algorithm id, footer length, and footer authentication. It also has explicit logical
length handling that subtracts the footer from the file length.

Parquet PME uses a mix of Parquet footer key metadata, Parquet key-value metadata markers, and
OpenSearch `WriterFileSet` metadata. The contract is not versioned yet and stores KMS context as a
plain string marker.

Impact:

- Lucene has a concrete compatibility surface for encrypted files.
- Parquet needs a versioned metadata contract, bounded metadata sizes, field validation, and clear
  rules for missing or mismatched metadata.

### 8. Operational Lifecycle

Lucene storage encryption has lifecycle features beyond the basic crypto path:

- Node-level key cache and expiry settings.
- Periodic key health monitoring and index block management.
- Resize/clone keyfile copying.
- Cache invalidation on file deletion and directory close.
- Translog encryption through `CryptoEngineFactory`.
- YAML REST tests, integration tests, concurrency tests, and JMH benchmarks.

Parquet PME has early lifecycle integration:

- Fail-fast registry/provider validation on engine creation.
- Writer-scoped native encryption configuration.
- Basic encrypted metadata/payload read tests.
- Existing Parquet writer checksum registration remains intact.

Impact:

- Parquet has a smaller, cleaner prototype surface.
- It has not yet absorbed the operational lessons already solved by storage encryption.

## Parquet PME Strengths

- It uses standard Parquet Modular Encryption rather than a custom Parquet-adjacent format.
- It delegates key generation to the OpenSearch `MasterKeyProvider`, avoiding custom KDF or RNG logic
  in the Parquet plugin.
- The Java `ParquetModularEncryptionConfig` defensively copies key arrays on construction and access.
- The FFM boundary validates all-or-nothing encryption payloads and rejects partial configuration.
- The encrypted write path preserves the existing VSR/Arrow batching architecture.
- The Rust implementation keeps plaintext and encrypted paths in the same writer setup path, reducing
  branch complexity after writer creation.
- Prototype read tests verify that encrypted metadata and payload row iteration work with the correct
  footer key and fail with the wrong key.
- `ParquetWriter.flush()` registers PME metadata in `WriterFileSet`, which is a useful hook for future
  reader planning and remote-store metadata propagation.

## Parquet PME Weaknesses and Risks

### High Priority

- The raw footer key is generated at `ParquetIndexingEngine` construction and reused by all writers
  created by that engine. This differs from the README wording around file key material and from
  Lucene's per-file derived-key model.
- Because `ParquetModularEncryptionConfig` is an engine field, the raw key can remain live for the
  entire shard engine lifetime. Lucene also keeps decrypted key material in cache, but that cache has
  explicit lifecycle and expiry controls; Parquet does not yet.
- Reader rehydration is incomplete. The persisted wrapped key is not yet unwrapped through
  `MasterKeyProvider.decryptKey(...)`; bridge reads require the raw key in memory.
- Restart and rotation behavior is not safe yet. A new engine instance can generate a new key while
  old encrypted files still require their original footer key.
- Java boundary validation only checks non-null/non-empty byte arrays. It does not enforce AES key
  lengths of 16, 24, or 32 bytes before crossing FFM.
- Plaintext key bytes live in Java byte arrays, FFM native memory segments, and Rust `Vec<u8>` without
  an explicit wipe/zeroization lifecycle.
- The metadata contract is not versioned. There is no single parser enforcing required fields,
  supported versions, maximum sizes, or fail-closed mismatch behavior.

### Medium Priority

- KMS context is stored as plain string metadata. If the context contains tenant or business details,
  those details may leak through metadata surfaces.
- The code duplicates PME metadata in Parquet key-value metadata and OpenSearch `WriterFileSet`
  metadata with slightly different keys (`enabled` vs `encrypted`, length markers vs base64 wrapped
  key). That should become one documented contract with clear ownership.
- Settings semantics may diverge from `opensearch-storage-encryption`, especially around
  `key_provider` vs `key_provider_type`.
- `initializeEncryption(...)` calls `CryptoHandlerRegistry.fetchCryptoHandler(...)` for validation and
  then separately calls `CryptoKeyProviderPlugin.createKeyProvider(...)` to generate the key pair. That
  keeps validation fail-fast, but it may create extra provider/client objects compared with a single
  provider-resolution path.
- There is no Parquet-specific equivalent of Lucene's key health monitoring, key cache expiry, or
  operational failover story.
- The current bridge decrypt APIs expose row-count/stat style reads, not the normal query path.
- Failure behavior around missing metadata, mismatched KMS identity, corrupted wrapped keys, and
  unsupported key lengths is not yet covered by a full negative test matrix.

### Lower Priority

- Rust writer state is globally keyed by filename in `WRITER_MANAGER` and `FILE_MANAGER`; this is
  consistent with the existing prototype but deserves stress coverage under retries and close/error
  paths.
- Encrypted empty-file behavior and checksum semantics should be explicitly documented because the
  encrypted Parquet footer can make "empty" files non-empty at the byte level.
- Metadata key names should be collected as constants shared by writer and reader code to avoid drift.

## Lucene Storage Encryption Lessons Applicable to Parquet

- Persist wrapped key material independently from process lifetime and prove restart reads.
- Use a versioned, bounded metadata format with magic/version fields and fail-closed parsing.
- Separate root data key lifecycle from per-file encryption key lifecycle.
- Derive or generate file-unique keys; do not accidentally reuse file encryption keys because a writer
  config object is cached at shard scope.
- Cache unwrapped keys with explicit expiry/refresh policy, and define behavior for KMS outages.
- Test resize, clone, snapshot/restore, shard migration, and mixed old/new key scenarios.
- Treat cache invalidation and close/error paths as part of the crypto design, not just performance.

## Recommended Production Roadmap

### Phase 1: Contract and Guardrails

- Move PME metadata keys into Java constants and Rust constants or a shared generated contract.
- Add a versioned metadata envelope with required fields:
  - contract version
  - provider type/name
  - key id/ARN
  - wrapped footer key
  - encryption context hash or opaque context id
  - algorithm/key length
  - optional file identity/AAD fields
- Enforce raw key lengths of 16, 24, or 32 bytes at Java construction and FFM parsing.
- Bound wrapped key and string metadata lengths.
- Add negative tests for partial payloads, invalid key lengths, missing fields, malformed base64,
  wrong provider metadata, and wrong key material.

### Phase 2: File-Key Lifecycle

- Decide whether keys are per file, per writer generation, or per shard.
- Prefer per-file key generation at `ParquetWriter` or native writer creation time.
- If key reuse is intentional, document the security rationale and rotation behavior.
- Add explicit zeroization/wiping for Java copies, FFM buffers where possible, and Rust key vectors.

### Phase 3: Reader Rehydration

- Build a reader-side metadata parser that loads wrapped footer key metadata from the Parquet file or
  `WriterFileSet`.
- Reconstruct `CryptoMetadata` and call `MasterKeyProvider.decryptKey(...)` to obtain the raw footer
  key on demand.
- Validate KMS identity/context metadata before attempting decrypt.
- Wire decrypted readers into the normal Parquet query/execution path.
- Add restart tests proving files written before restart remain readable.

### Phase 4: Operations

- Define rotation: new files get new keys, old files remain readable by stored wrapped keys.
- Add telemetry/audit hooks for data key generation and unwrap events.
- Define KMS outage behavior. For production, prefer fail-closed reads/writes unless a documented
  cached-key grace period is intentionally adopted.
- Add snapshot/restore, clone/resize, shard relocation, and remote-store tests.

### Phase 5: Performance and Reliability

- Benchmark encrypted vs plaintext Parquet writes and reads.
- Stress test concurrent VSR rotation, native writer lifecycle, and encrypted flush/sync.
- Add memory pressure tests around key material, Arrow buffers, and Rust writer manager maps.

## Bottom Line

The Parquet PME prototype is pointed in the right direction because it uses the real Parquet
encryption mechanism and OpenSearch's provider abstractions. The biggest architectural gap is not the
write path; it is durable key recovery and integrated reads. The Lucene storage encryption plugin is
the better model for lifecycle, settings validation, key caching, and operations, while Parquet should
keep its own file-format-native encryption rather than copying Lucene's custom footer format.
