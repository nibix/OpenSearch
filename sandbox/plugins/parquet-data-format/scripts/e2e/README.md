# PME End-to-End Integration Tests

Shell-script end-to-end integration tests for Parquet Modular Encryption (PME)
against a real single-node OpenSearch cluster, using the `mock-pme-kms` plugin
as a dummy KMS (no external service required).

## Prerequisites

| Requirement | Notes |
|---|---|
| JDK 25 | Set `JAVA_HOME`. JDK 21 minimum, 25 recommended for sandbox modules. |
| Rust + cargo | For the native library build step. |
| `curl` | REST test calls. |
| `jq` | JSON assertion parsing. |
| Port 9200 | Must be free on localhost. |

## Files

```
scripts/e2e/
├── setup-cluster.sh              Build plugins and start OpenSearch (background).
├── test-pme.sh                   Run REST tests against a running cluster.
├── test-pme-kms-unavailable.sh   Verify encrypted indices are inaccessible without KMS.
├── teardown.sh                   Stop the cluster started by setup-cluster.sh.
└── run-all.sh                    Full lifecycle: build → start → test → KMS-off → stop.

sandbox/plugins/mock-pme-kms/
└── ...                           Dummy KMS plugin (no real KMS, identity decryption).
```

## Quickstart

```bash
# From the repo root — full lifecycle in one command:
./sandbox/plugins/parquet-data-format/scripts/e2e/run-all.sh

# Skip the Gradle build if plugins are already assembled:
./sandbox/plugins/parquet-data-format/scripts/e2e/run-all.sh --skip-build

# Keep the cluster running after the test run (for manual inspection):
./sandbox/plugins/parquet-data-format/scripts/e2e/run-all.sh --keep-cluster
```

## Step-by-step

```bash
# 1. Build and start the cluster (blocks until cluster is healthy):
./sandbox/plugins/parquet-data-format/scripts/e2e/setup-cluster.sh

# 2. Run the tests against the running cluster:
./sandbox/plugins/parquet-data-format/scripts/e2e/test-pme.sh

# 3. Stop the cluster when done:
./sandbox/plugins/parquet-data-format/scripts/e2e/teardown.sh
```

## Test against an existing cluster

If you already have a cluster running elsewhere:

```bash
./sandbox/plugins/parquet-data-format/scripts/e2e/test-pme.sh --host=http://my-host:9200
```

The test script checks that both `parquet-data-format` and `mock-pme-kms` are
installed. Use `GET /_cat/plugins` to verify.

## What is tested

| Test | Description |
|---|---|
| T1 | Create encrypted Parquet index with `mock-pme` key provider |
| T2 | Index 5 documents + force-refresh |
| T3 | Match-all search returns all 5 documents |
| T4 | Term query on encrypted field returns correct subset + correct source values |
| T5 | Aggregation (SUM + COUNT) over encrypted numeric field returns correct result |
| T6 | Delete encrypted index — subsequent search returns HTTP 404 |
| T7 | Two independent encrypted indices each have separate keyfiles and isolated data |
| T8 | Plain (unencrypted) index works normally alongside PME indices |
| T9 | Create persistent encrypted index readable with KMS; left for Phase 4 |
| T9-A | (Phase 4, no-KMS cluster) Encrypted index returns error or 0 hits |
| T9-B | (Phase 4, no-KMS cluster) Creating new encrypted index is rejected |
| T9-C | (Phase 4, no-KMS cluster) Plain index remains fully readable |

## The mock-pme-kms plugin

`MockPmeKmsPlugin` registers the key provider type `"mock-pme"`. It uses a pure
Java implementation with no Mockito dependency — suitable for installation in a
real node.

Key behaviour:
- `generateDataPair()` — generates a fresh 32-byte key via `SecureRandom`.
  The "encrypted" copy is identical to the raw key (identity wrapping).
- `decryptKey(bytes)` — returns the input unchanged.

This satisfies the PME round-trip contract (`decryptKey(encryptedKey) == rawKey`)
without any external KMS service.

**Not for production use.** Any party that can read the keyfile can decrypt the
data without KMS involvement.

## Index settings used by the tests

```json
{
  "settings": {
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "parquet",
    "index.store.parquet.crypto.key_provider": "test",
    "index.store.parquet.crypto.key_provider_type": "mock-pme"
  }
}
```

## Troubleshooting

**Cluster fails to start**
Check `scripts/e2e/.cluster.log` for Gradle build output. For the OpenSearch server
startup log, check:
```
build/testclusters/runTask-0/logs/runTask.log
```

**Where to find TRACE log messages**
`.cluster.log` only captures Gradle process output (build messages). The actual
OpenSearch server log — including all TRACE-level PME encryption messages — is at:
```
build/testclusters/runTask-0/logs/runTask.log
```
`setup-cluster.sh` enables TRACE logging for the PME packages via
`PUT /_cluster/settings` after the cluster starts, and prints the server log path.
To follow live:
```bash
tail -f build/testclusters/runTask-0/logs/runTask.log | grep -E "TRACE|PME|pme|parquet.*encrypt|datafusion.*encrypt"
```

**`parquet-data-format` or `mock-pme-kms` plugin not found**
Make sure the Gradle build completed and the `run` task includes both plugins:
```
-PinstalledPlugins='["parquet-data-format", "mock-pme-kms"]'
```

**`jq: command not found`**
Install jq:
```bash
brew install jq        # macOS
apt-get install jq     # Debian/Ubuntu
```

**Port 9200 already in use**
Stop any existing OpenSearch/Elasticsearch process or change the port via
cluster settings (requires changes to `setup-cluster.sh` and `test-pme.sh`).




