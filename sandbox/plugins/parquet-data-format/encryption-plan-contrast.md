# Parquet PME Plan Contrast and Scope Assessment

This document contrasts the provided "Parquet Modular Encryption Support" plan with the implementation
review in `encryption-architecture-review.md` and the control-flow diagrams in
`encryption-mermaid-diagrams.md`.

The short version: the plan has the right broad work packages, but it underweights three areas that
the implementation review found to be central:

- durable reader-side key rehydration after restart
- a strict, versioned file metadata contract
- the difference between "single key per index" and "single raw file encryption key reused by every
  writer in an engine"

It also treats several architecture decisions as small RFC or verification tasks, while they are
actually the decisions that determine whether the implementation can be made production-safe.

## Current Prototype Baseline

The current prototype already proves more than a pure smoke test:

- It obtains key material through OpenSearch crypto plumbing.
- It passes a raw footer key and wrapped footer key over Java FFM into Rust.
- It enables real Parquet encryption using Rust Parquet `FileEncryptionProperties`.
- It persists wrapped key material in Parquet footer key metadata and in `WriterFileSet` metadata.
- It can read encrypted footer metadata and iterate encrypted payload rows through bridge-level helper
  APIs when the raw footer key is already available.
- It has targeted tests for encrypted metadata reads, encrypted row-count reads, and wrong-key failure.

The current prototype does not yet prove:

- restart-stable key recovery
- reader-side unwrap through `MasterKeyProvider.decryptKey(...)`
- integration with the normal query/execution path
- per-file key generation or derivation
- versioned metadata parsing
- production AAD policy
- key zeroization
- snapshot/restore behavior
- merge behavior
- cluster-level operational failure modes

That distinction matters for estimating. The remaining work is less about "can Parquet encryption
work?" and more about "can OpenSearch reliably discover, hydrate, validate, and use the right key for
every encrypted file under all lifecycle flows?"

## Assessment of Plan Assumptions

### Assumption: Single Key per Index, Used to Derive File Keys

This is a good production direction if it means:

- one index-level KMS/master-key identity
- one persisted wrapped index/root data key, or a KMS-generated data key per file
- per-file encryption keys derived or generated from file-specific identity
- file metadata carries enough information to recover the file key after restart

It is risky if it means:

- one raw data key is generated at engine construction
- that same raw key is passed as the Parquet footer key for every Parquet file
- old files can only be read while that engine-scoped raw key remains in memory

The current implementation is closer to the risky interpretation. `ParquetIndexingEngine` creates one
`ParquetModularEncryptionConfig` during engine construction and passes it to all `ParquetWriter`
instances. That means a raw key can live for the full shard engine lifetime and can be reused across
writer generations.

Recommended correction:

- Make the assumption explicit as "single index-level key identity; per-file Parquet encryption key."
- Generate or derive file-specific Parquet keys at file/writer creation, not engine construction.
- Store only wrapped file key metadata or derivation metadata with each file.
- Keep the metadata contract rotation-compatible even if key rotation is deferred.

### Assumption: No Column-Specific Encryption

This is aligned with the current OpenSearch-only scope and should reduce risk. Column-specific
encryption would complicate schema evolution, query planning, stats, and field-level authorization.

Recommended correction:

- Keep this as a hard v1 non-goal.
- Still decide whether Parquet footer metadata and column metadata are encrypted or plaintext under
  the chosen Parquet encryption properties.
- Add tests proving sensitive field names, min/max stats, and key-value metadata exposure are
  acceptable under the chosen mode.

### Assumption: No Key Rotation

This is acceptable as a v1 feature constraint, but it should not become a metadata shortcut.

Even without rotation, the implementation must support:

- reading files written before a node restart
- reading files written before an engine recreation
- reading files in a mixed snapshot where different files may have different wrapped keys because of
  retries, merges, or future rollout changes
- identifying which KMS key and context to use for each file

Recommended correction:

- State "no operator-initiated key rotation" rather than "no key versioning."
- Include key id/provider id/context hash in per-file metadata from the beginning.
- Store enough metadata so future key rotation can be implemented without rewriting every file
  contract.

### Assumption: AES GCM for Metadata and AES CTR for Payload

This mirrors the Lucene storage encryption design, but Parquet PME should be described in Parquet
terms rather than Lucene terms. The current Rust code delegates mode selection to Apache Parquet's
encryption implementation through `FileEncryptionProperties`.

The plan should avoid implying that OpenSearch will hand-roll AES-GCM metadata and AES-CTR payload
handling in the Parquet plugin. The better design is:

- choose and document the Parquet encryption algorithm/mode exposed by the Rust Parquet library
- configure it explicitly if the library supports multiple modes
- test that metadata and payload tampering fail as expected
- treat Lucene's AES-GCM/CTR behavior as a reference point, not as the implementation model

Recommended correction:

- Replace this assumption with "Use the Parquet library's authenticated modular encryption mode; document
  the exact algorithm/mode and its integrity guarantees."
- Add corruption tests for encrypted footer metadata and encrypted payload bytes.

### Assumption: No Further Integrity Checks Beyond PME AAD

This is plausible, but only if AAD is designed carefully and verified with negative tests.

AAD should protect against at least:

- file swap within a shard
- file swap across shards
- stale file metadata reused after restore
- wrong index UUID or shard ID
- wrong KMS context
- accidental plaintext/encrypted path confusion

Recommended correction:

- Define AAD as a production contract, not a write-path implementation detail.
- Include cluster/index/shard/file identity and metadata contract version.
- Decide how AAD behaves across snapshot/restore, clone, split, and shrink. If restored indices get
  new index UUIDs, either AAD must intentionally bind to stable original identity or restore must
  rewrite/re-encrypt files.

## Workstream-by-Workstream Contrast

### Minimal Prototype

Plan:

- Basic write and read flows only.
- No merging, snapshots, or additional integrity measures.
- Smoke tested, no elaborate test suite.
- Estimate: 32 hours.

Implementation insight:

- The existing prototype already covers real encrypted writes and bridge-level encrypted reads.
- It does not yet cover reader rehydration from stored wrapped key metadata.
- It does not validate the core production risk: can a process that no longer has the original raw key
  recover it from file metadata and KMS?

Contrast:

- If the plan refers to work already done, 32 hours is no longer the right remaining estimate.
- If the plan means "prototype the missing production reader rehydration shape," then 32 hours is
  reasonable and valuable.

Recommended acceptance criteria:

- Write encrypted file with a raw key that is not retained in process state.
- Read wrapped footer key metadata from file or `WriterFileSet`.
- Recreate `MasterKeyProvider` from metadata/settings.
- Call `decryptKey(...)` and read rows.
- Verify wrong KMS context or wrong wrapped key fails deterministically.

Suggested adjustment:

- Reframe this as "rehydration prototype" rather than "minimal PME prototype."
- Keep the estimate at 24-40 hours depending on whether the query path is included.

### RFC

Plan:

- Create an RFC issue based on prototype insights.
- Fix contracts for PME footer format, AAD, runtime context, and hydrated key lifecycle.
- Estimate: 8 hours.

Implementation insight:

- This RFC is the central architectural gate. The review found that metadata, key lifecycle, settings
  semantics, and read rehydration are the highest-risk areas.
- "PME footer format" is not quite the right phrase if using Apache Parquet's format. The OpenSearch
  contract should define how OpenSearch stores and interprets provider metadata, wrapped keys, AAD
  inputs, and version fields around the Parquet library's encryption metadata.

Contrast:

- 8 hours is too low if the RFC is meant to settle contracts across Parquet, DataFusion/query
  execution, remote store, snapshots, and possibly Lucene encryption.
- 8 hours is plausible only for drafting, not for alignment and decision closure.

Recommended RFC scope:

- Metadata location: Parquet footer key metadata, `WriterFileSet`, catalog snapshot, or a combination.
- Metadata schema: required fields, version, maximum lengths, binary vs base64 encoding, fail-closed
  behavior.
- Key hierarchy: KMS key, index/root data key, file key, derivation material.
- Key generation point: engine, writer, native writer, or separate key service.
- Reader lifecycle: where unwrapped keys live, cache TTL, close/zeroization, concurrency model.
- AAD identity: cluster/index/shard/file/generation and restore semantics.
- Settings compatibility: `key_provider` vs `key_provider_type` behavior.
- Mixed segment handling: plaintext and encrypted Parquet files in the same reader snapshot.
- Failure semantics: missing KMS, disabled key, bad context, corrupt footer, wrong wrapped key.

Suggested adjustment:

- Estimate 16-24 hours for a real RFC cycle.
- Make it a prerequisite for production write/read implementation, not merely a follow-up to the
  prototype.

### Write Path

Plan:

- Production-ready PME write path.
- VSR confidentiality hardening and zeroing.
- AAD provisioning.
- KMS key suitability verification.
- Unit/integration tests and review.
- Estimate: 32 hours.

Implementation insight:

- The current write path already creates encrypted Parquet files.
- The main production gaps are not basic writing, but key granularity, AAD, metadata contract,
  key-length validation, and key cleanup.
- The current raw key is generated at engine initialization, not writer/file creation.

Contrast:

- 32 hours is plausible if the existing engine-scoped key model is retained.
- It is low if the write path is corrected to create/derive file-unique keys and emit a versioned
  metadata envelope.

Recommended write-path scope:

- Move key generation/derivation to file creation or introduce a file-key derivation service.
- Add strict validation of AES key length before FFM and again in Rust.
- Add AAD fields to the Java config and Rust encryption options.
- Make Parquet encryption properties explicit, including footer/payload metadata policy.
- Define and emit one canonical OpenSearch PME metadata envelope.
- Avoid duplicate or divergent metadata keys between Parquet key-value metadata and `WriterFileSet`.
- Zero or shorten lifetimes of key material in Java arrays, FFM segments, and Rust vectors.
- Add tests for invalid keys, partial FFM payloads, AAD mismatch where supported, and metadata
  round-trips.

Suggested adjustment:

- Estimate 40-56 hours for production write path if file-key lifecycle and metadata contract are
  included.
- Keep 32 hours only if this is limited to hardening the existing prototype write path.

### Merge Path

Plan:

- Dependent on non-encrypted Parquet merge implementation.
- Provision AAD data.
- Tests and review.
- Estimate: 20 hours.

Implementation insight:

- Merge semantics are a major unknown. A Parquet merge may rewrite rows into new files, copy row
  groups, combine metadata, or rely on an external execution engine.
- Encryption complicates merge because new output files need new metadata, new AAD, and possibly new
  file keys.
- If merge reads encrypted input and writes encrypted output, the merge path is both read path and
  write path plus lifecycle management.

Contrast:

- The plan correctly marks this as rough, but 20 hours is only plausible after plaintext merge is
  stable and if encrypted merge is "decrypt inputs, write a new encrypted output" using existing
  read/write primitives.
- If merge tries to preserve encrypted row groups without decrypt/rewrite, the complexity is much
  higher and must be validated against Parquet encryption rules.

Recommended merge scope:

- Decide whether merge is rewrite-based or metadata/row-group-copy-based.
- If rewrite-based, use the same reader rehydration and writer file-key lifecycle as normal query and
  write paths.
- Define AAD for merged output files. Do not reuse source file AAD if output file identity changes.
- Add tests for mixed encrypted/plaintext input segments if that state can occur.
- Add failure tests for one unreadable source file during merge.

Suggested adjustment:

- Treat 20 hours as a placeholder.
- Re-estimate after plaintext merge design is available.
- Planning range: 24-64 hours depending on merge architecture.

### Read Path

Plan:

- Harder than write path due to random access.
- Retrieve encryption information from Parquet footer and hydrate keys with KMS.
- Manage runtime context data about Parquet segments.
- Implement failure modes and AAD verification.
- Excludes buffer pools/optimization.
- Estimate: 48 hours.

Implementation insight:

- The review agrees that read path is the hardest part.
- However, the difficulty is not only random access. It is also metadata discovery, key unwrap,
  cache/lifecycle, mixed encrypted/plaintext snapshots, and integration into the actual query path.
- The current bridge helper API reads metadata and row counts with a raw key, but the normal
  query/execution reader does not use it.

Contrast:

- 48 hours is likely low for a production read path.
- If the read path must include DataFusion/query integration, restart-stable key hydration, and
  failure-mode tests, the scope is closer to a primary project phase than a contained feature task.

Recommended read-path scope:

- Build a per-file PME metadata parser.
- Resolve provider settings and create `MasterKeyProvider` without relying on engine-held raw keys.
- Call `decryptKey(...)` on wrapped footer key metadata.
- Cache hydrated keys or decryption properties with explicit lifetime and close behavior.
- Pass file-to-key mapping through Java FFM into the Rust/DataFusion reader.
- Support plaintext files and encrypted files in one snapshot if necessary.
- Define fail-closed errors for missing metadata, wrong key, wrong AAD, provider unavailable, and
  corrupted footer.
- Add restart tests and wrong-key tests at the real query/execution layer.

Suggested adjustment:

- Estimate 72-96 hours for a production read path.
- Keep 48 hours only for a non-optimized reader that bypasses the final query execution path or
  handles a single encrypted file at a time.

### Snapshot/Restore

Plan:

- Implement and verify snapshot/restore flows including re-encryption using SSE.
- Define solution, implement, test against AWS SSE.
- Estimate: 32 hours.

Implementation insight:

- Snapshot/restore is not just storage encryption at the repository layer.
- Parquet files are already client-side encrypted by PME. SSE adds object-store-side encryption, but
  it does not solve Parquet key metadata or reader rehydration.
- If restore changes index identity, AAD binding becomes a central design issue.
- If snapshots copy encrypted Parquet files as ciphertext, the wrapped file key metadata must remain
  available and valid after restore.

Contrast:

- The phrase "re-encryption using SSE" needs clarification. SSE encrypts snapshot objects in the
  repository; it is not the same as re-encrypting Parquet PME file keys.
- If the desired behavior is "snapshot encrypted files as-is and rely on repository SSE as an
  additional layer," the scope is mostly metadata preservation and integration testing.
- If the desired behavior is "decrypt and re-encrypt Parquet files during snapshot/restore," the scope
  is much larger and introduces KMS, AAD, and data-copy risks.

Recommended snapshot/restore decisions:

- Are Parquet files snapshotted as PME ciphertext, or are they re-encrypted with new PME keys?
- Does restored index identity preserve AAD inputs or rewrite them?
- Where is wrapped footer key metadata stored in the snapshot catalog?
- How does restore fail if KMS is unavailable?
- Does repository SSE use the same or different KMS key from PME?

Suggested adjustment:

- Split into an RFC/design spike and an implementation phase.
- Planning range: 40-80 hours depending on whether PME re-encryption is required.

### Separate RFC About Overall Encryption Architecture Including Lucene

Plan:

- Revisit overall encryption architecture.
- Clarify encrypted translog across Lucene and Parquet.
- Clarify shared buffer pooling and related dependencies.
- Estimate: 6 hours.

Implementation insight:

- This is too important to be a 6-hour side RFC.
- Translog encryption is currently provided by `opensearch-storage-encryption` through an
  `EnginePlugin` path for `cryptofs` indices. The Parquet indexing engine may use the shared
  OpenSearch translog, but the activation and ownership model still needs to be made explicit.
- Buffer pool reuse is not just code reuse. It creates module-boundary, lifecycle, security, and
  plaintext-cache questions.

Contrast:

- The plan is right that this architecture question exists.
- The estimate and sequencing are too light. This decision should happen before committing to read
  buffering or translog scope.

Recommended RFC questions:

- Is storage encryption a general OpenSearch encryption service, or does it remain a Lucene store
  plugin?
- Who owns translog encryption when a non-Lucene indexing engine is active?
- Does enabling Parquet PME require `cryptofs`, or can it be independent?
- Should Parquet depend on `opensearch-storage-encryption`, or should common pieces move into a
  shared module/library?
- Is plaintext block caching acceptable for Parquet encrypted reads, and under what memory-clearing
  policy?
- How are KMS health monitoring, key caching, metrics, and audit hooks shared?

Suggested adjustment:

- Estimate 16-32 hours for meaningful architecture alignment.
- Put it before "Read Path with Buffer Pools" and before final translog claims.

### Read Path with Buffer Pools

Plan:

- Add buffer pools into read path.
- Investigate reuse of existing Lucene encryption buffer pool implementation.
- Find a module structure for reuse if possible.
- Estimate: 24 hours.

Implementation insight:

- The Lucene buffer pool is tightly tied to Lucene `Directory`, block-aligned file reads,
  `IndexInput`, `RefCountedMemorySegment`, direct I/O, `BlockCache`, read-ahead, and a custom file
  footer.
- Parquet/DataFusion readers may already have their own buffering, projection, predicate pushdown, and
  page/row-group access patterns.
- Reusing Lucene's plaintext block cache could undermine some of Parquet PME's file-format-level
  guarantees if not carefully scoped and zeroized.

Contrast:

- Buffer pooling is probably an optimization phase, not part of the first production correctness path.
- 24 hours is reasonable for an investigation spike, not for a reusable implementation.

Recommended sequencing:

- Implement correct reader rehydration first.
- Benchmark before designing a shared buffer pool.
- Determine whether the hot path is KMS unwrap, Parquet decryption, DataFusion scanning, Arrow
  allocation, or I/O.
- Only then decide whether Lucene's buffer pool is useful.

Suggested adjustment:

- Estimate 16-24 hours for a spike.
- Estimate implementation separately after benchmark results.

### Verification of Additional Flows

Plan:

- Verify shard relocation, recovery, and reindexing.
- Estimate: 12 hours.

Implementation insight:

- These flows may not require code changes, but encrypted metadata and AAD can make them fail in
  non-obvious ways.
- Recovery and relocation are especially sensitive if AAD includes node-local paths, index UUIDs, or
  shard path details.
- Reindexing is likely less risky because it reads plaintext through query APIs and writes new files,
  but it still depends on the read path being fully integrated.

Contrast:

- 12 hours is low for integration tests that need stable cluster setup, negative cases, and possibly
  restarts.
- The plan should separate "test happy path" from "test fail-closed behavior."

Recommended verification matrix:

- shard relocation with encrypted Parquet files
- primary recovery after restart
- replica recovery if supported by the Parquet engine path
- reindex from encrypted to encrypted
- reindex from encrypted to plaintext, if allowed
- mixed old/new metadata version if rolling upgrade is relevant
- recovery with missing KMS provider
- recovery with wrong encryption context

Suggested adjustment:

- Estimate 24-40 hours for meaningful coverage.

### KMS Integration Testing

Plan:

- Test against AWS KMS.
- Estimate: 6 hours.

Implementation insight:

- The repository already has AWS KMS plugin code. The challenge is not just calling KMS once; it is
  validating the full metadata and encryption context flow under real AWS behavior.
- AWS KMS integration tests need credentials, region/key setup, CI gating rules, cleanup, and likely
  skip behavior for local development.

Contrast:

- 6 hours is enough for a manual smoke test.
- It is low for repeatable integration tests.

Recommended KMS tests:

- generate data key for writer
- persist wrapped footer key
- restart or recreate reader context
- decrypt wrapped key through AWS KMS
- fail with wrong encryption context
- fail with disabled/unauthorized key, if feasible in the test environment
- verify no raw key is logged or persisted

Suggested adjustment:

- Estimate 16-24 hours for repeatable integration testing.

### Benchmarking

Plan:

- Compare encrypted product to base implementation for ingestion and search.
- No optimization included.
- Estimate: 16 hours.

Implementation insight:

- Benchmarking is necessary, but it should happen after correctness, before buffer pool design.
- The current plan puts buffer pool read optimization before benchmarking. That may optimize the wrong
  layer.

Contrast:

- 16 hours is plausible for a first benchmark pass if benchmark harnesses already exist.
- It may be low for representative search benchmarks across encrypted read paths.

Recommended benchmark dimensions:

- plaintext Parquet write vs encrypted Parquet write
- bridge-level encrypted read vs query-path encrypted read
- warm vs cold key cache
- warm vs cold file/page cache
- KMS unwrap on every file vs cached unwrap
- small files vs large files
- many writer generations vs few large files
- search/query latency with projection and predicate pushdown

Suggested adjustment:

- Estimate 24-40 hours for a reliable benchmark report.
- Use results to decide whether read buffer pooling is necessary.

## Missing or Underweighted Workstreams

### 1. Metadata/Catalog Contract Implementation

The plan mentions footer format and runtime context in the RFC, but it does not list implementation of
the metadata contract as its own production workstream.

This should include:

- canonical PME metadata DTO
- serialization in `WriterFileSet` or catalog snapshot
- Parquet footer metadata parser
- versioning and migration policy
- maximum sizes and validation
- tests for missing/malformed metadata

This work likely cuts across write path, read path, snapshot/restore, and recovery.

### 2. Settings Compatibility

The plan does not explicitly address that the current Parquet prototype and
`opensearch-storage-encryption` can interpret `index.store.crypto.key_provider` differently.

This should be fixed or documented before KMS integration testing, otherwise tests may pass with one
configuration style and fail in real deployments.

### 3. Key Cache and Health Model

The plan says "runtime lifecycle of hydrated keys," but does not allocate implementation time for:

- key cache ownership
- TTL/expiry policy
- KMS outage behavior
- index block behavior, if any
- metrics/audit hooks
- cleanup on shard close

Lucene storage encryption has `NodeLevelKeyCache` and `MasterKeyHealthMonitor`; Parquet currently has
no equivalent.

### 4. Mixed Plaintext/Encrypted Segments

During rollout, testing, and failures, the system may see:

- old plaintext Parquet files
- new encrypted Parquet files
- empty files
- files with older metadata versions

The reader should explicitly handle or reject each case.

### 5. Security Review and Tamper Tests

The plan mentions AAD verification, but production readiness should include:

- corrupted footer metadata
- corrupted payload bytes
- swapped wrapped keys
- swapped files across shards
- wrong KMS context
- wrong provider type
- metadata downgrade/version tampering
- raw key leakage checks in logs and metadata

### 6. Documentation and Operator Guidance

The plan is implementation-heavy. Operators will need:

- settings examples
- failure modes
- restore behavior
- KMS permissions
- no-rotation limitation
- compatibility with storage encryption and translog encryption
- benchmark expectations

## Suggested Revised Sequencing

The plan's sequencing is broadly good, but I would reorder a few gates:

1. Close current prototype notes.
   - Treat the existing code as the minimal write/read proof.
   - Add only a small rehydration spike if needed.

2. Do the PME contract RFC before production implementation.
   - Metadata schema.
   - AAD policy.
   - key hierarchy.
   - read lifecycle.
   - settings semantics.

3. Do the overall encryption architecture RFC early.
   - Especially translog ownership and shared KMS/key-cache components.
   - Do this before buffer pool reuse decisions.

4. Implement metadata contract and file-key lifecycle.
   - This should be a dedicated implementation slice, not hidden inside write/read.

5. Harden write path.
   - Per-file key generation/derivation.
   - AAD.
   - validation.
   - zeroization.

6. Implement production read path.
   - Wrapped key parsing.
   - KMS unwrap.
   - hydrated key context.
   - query path integration.
   - failure modes.

7. Verify restart/recovery/relocation/reindexing.
   - These should follow the actual read path.

8. Decide merge strategy once plaintext merge exists.
   - Re-estimate then.

9. Decide snapshot/restore behavior.
   - Especially AAD identity and SSE vs PME re-encryption.

10. Benchmark.
    - Use results to decide whether read buffer pools are needed.

11. Buffer pool optimization.
    - Treat as performance work, not correctness work.

## Estimate Recalibration

The provided total is 236 hours. My read is that 236 hours is plausible for an optimistic v1 if:

- no production translog ownership changes are needed
- no Parquet merge path is implemented yet
- snapshot/restore only verifies metadata preservation plus repository SSE
- read path integration is narrow
- buffer pooling is only investigated
- key rotation remains out of scope

For a production-ready implementation matching the risks described in the review, the range is likely
higher.

| Workstream | Plan estimate | Assessment | Suggested planning range |
|---|---:|---|---:|
| Minimal prototype | 32h | Mostly already exists; rehydration spike still valuable | 0-40h |
| PME RFC | 8h | Too low for contract closure | 16-24h |
| Write path | 32h | Low if per-file keys, AAD, metadata, zeroization included | 40-56h |
| Merge path | 20h | Cannot estimate until plaintext merge is known | 24-64h |
| Read path | 48h | Likely low for query integration and failure modes | 72-96h |
| Snapshot/restore | 32h | Depends heavily on SSE vs PME re-encryption semantics | 40-80h |
| Overall encryption RFC | 6h | Too low for translog/shared architecture | 16-32h |
| Read buffer pools | 24h | Good spike estimate, not implementation estimate | 16-24h spike |
| Relocation/recovery/reindex verification | 12h | Low for cluster tests and negative cases | 24-40h |
| KMS integration testing | 6h | Manual smoke only; repeatable tests need more | 16-24h |
| Benchmarking | 16h | Plausible first pass; likely low for search/read analysis | 24-40h |

Conservative total range:

- Narrow v1 with deferred merge, narrow snapshot validation, no buffer implementation: roughly
  280-360 hours.
- Production v1 with read/query integration, metadata contract, restart tests, KMS tests, and
  meaningful operational verification: roughly 360-480 hours.
- Full scope including merge, snapshot/restore semantics, architecture refactoring, and buffer-pool
  implementation: likely 480+ hours.

These are not precise forecasts; they are risk-adjusted planning ranges. The biggest swing factors
are merge architecture, snapshot/restore semantics, and whether common encryption infrastructure must
be extracted from `opensearch-storage-encryption`.

## Recommended Edits to the Plan

I would update the plan as follows:

1. Replace "single key per index" with:
   "single index-level KMS/key identity; per-file Parquet encryption keys generated or derived under a
   versioned metadata contract."

2. Replace "no key rotation" with:
   "no operator-driven rotation in v1, but per-file metadata includes key identity/version so rotation
   can be added later."

3. Replace "AES GCM metadata and AES CTR payload" with:
   "use the Parquet library's configured PME mode; document and test its metadata/payload integrity
   guarantees."

4. Add a dedicated workstream:
   "Metadata and key rehydration contract implementation."

5. Expand the RFC section to include settings semantics and snapshot/restore AAD identity.

6. Move the overall encryption architecture RFC before buffer pool reuse.

7. Move benchmarking before buffer pool implementation.

8. Split snapshot/restore into:
   - design decision: ciphertext copy plus SSE vs PME re-encryption
   - metadata preservation implementation
   - AWS/SSE integration validation

9. Add explicit restart tests to the read path.

10. Add tamper/fail-closed tests to both write/read and KMS testing.

## Final Take

The plan is directionally strong. It identifies the right big buckets: prototype, RFC, write, read,
merge, snapshot/restore, translog architecture, buffering, KMS, and benchmarking.

The main correction is that Parquet PME production readiness is dominated by metadata and key lifecycle,
not by the ability to turn encryption on in the writer. The current prototype already turns encryption
on. The real product work is making every encrypted file self-describing enough to be read later,
under the correct KMS context, through the normal query path, after restarts, relocation, recovery,
snapshot/restore, and eventually merge.

The plan should therefore promote metadata contract, file-key lifecycle, reader rehydration, and
cross-engine encryption ownership from supporting details into first-class deliverables.
