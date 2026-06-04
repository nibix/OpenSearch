#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"
CLUSTER_PID_FILE="${SCRIPT_DIR}/.cluster.pid"
CLUSTER_LOG_FILE_KMS="${SCRIPT_DIR}/.cluster-no-kms.log"

SKIP_BUILD_FLAG=""
KEEP_CLUSTER=false

for arg in "$@"; do
    case "${arg}" in
        --skip-build)   SKIP_BUILD_FLAG="--skip-build" ;;
        --keep-cluster) KEEP_CLUSTER=true ;;
        *) echo "[run-all] Unknown argument: ${arg}" >&2; exit 1 ;;
    esac
done

info() { echo "[run-all] $*"; }

# ---- ensure teardown on exit unless --keep-cluster ---------------------------

cleanup() {
    if [[ "${KEEP_CLUSTER}" == false ]]; then
        info "Tearing down cluster ..."
        bash "${SCRIPT_DIR}/teardown.sh"
        # Also remove the no-kms log file
        rm -f "${CLUSTER_LOG_FILE_KMS}"
    else
        info "Cluster kept running (--keep-cluster). Use teardown.sh when done."
    fi
}
trap cleanup EXIT

# ---- Phase 1: Start cluster with KMS -----------------------------------------

info "=== Phase 1: Start cluster (with mock-pme-kms) ==="
if ! bash "${SCRIPT_DIR}/setup-cluster.sh" ${SKIP_BUILD_FLAG}; then
    info "Cluster setup FAILED — aborting."
    exit 1
fi

# ---- Phase 2: Run main PME end-to-end tests ----------------------------------

info "=== Phase 2: Run PME end-to-end tests ==="
bash "${SCRIPT_DIR}/test-pme.sh"
TEST_EXIT=$?

if [[ ${TEST_EXIT} -ne 0 ]]; then
    info "Main PME tests FAILED (exit ${TEST_EXIT}) — skipping KMS-unavailable phase."
    exit ${TEST_EXIT}
fi

# ---- Phase 3: Restart cluster WITHOUT mock-pme-kms --------------------------
# The pme-t9-persist index (created in T9 of test-pme.sh) must still be present
# on disk so we can verify that its data is inaccessible without the key provider.

info "=== Phase 3: Restart cluster WITHOUT mock-pme-kms ==="
info "Stopping current cluster ..."
bash "${SCRIPT_DIR}/teardown.sh"

# Small grace period to ensure the OS releases the port before the next start.
sleep 3

info "Starting cluster WITHOUT mock-pme-kms (parquet-data-format only) ..."
GRADLEW="${REPO_ROOT}/gradlew"
if [[ ! -x "${GRADLEW}" ]]; then
    info "ERROR: gradlew not found at ${GRADLEW}" >&2
    exit 1
fi

"${GRADLEW}" \
    -Dsandbox.enabled=true \
    -PinstalledPlugins='["parquet-data-format"]' \
    -Dtests.opensearch.logger.org.opensearch.parquet.encryption=TRACE \
    -Dtests.opensearch.logger.org.opensearch.be.datafusion=TRACE \
    run \
    > "${CLUSTER_LOG_FILE_KMS}" 2>&1 &

NO_KMS_PID=$!
echo "${NO_KMS_PID}" > "${CLUSTER_PID_FILE}"
info "No-KMS cluster PID: ${NO_KMS_PID} (log: ${CLUSTER_LOG_FILE_KMS})"

# Wait for cluster to come up.
url="http://localhost:9200"
timeout=120
elapsed=0
info "Waiting for no-KMS cluster at ${url} (timeout ${timeout}s) ..."
while ! curl -sf "${url}/_cluster/health" -o /dev/null 2>/dev/null; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [[ ${elapsed} -ge ${timeout} ]]; then
        info "ERROR: No-KMS cluster did not start within ${timeout}s. Check ${CLUSTER_LOG_FILE_KMS}." >&2
        exit 1
    fi
done
info "No-KMS cluster is up."

# Enable TRACE logging for PME packages in the no-KMS cluster as well.
info "Enabling TRACE logging for PME packages in no-KMS cluster ..."
trace_resp=$(curl -s -w "\n%{http_code}" -X PUT "http://localhost:9200/_cluster/settings" \
    -H "Content-Type: application/json" \
    -d '{
      "transient": {
        "logger.org.opensearch.parquet.encryption": "TRACE",
        "logger.org.opensearch.be.datafusion": "TRACE"
      }
    }')
trace_status=$(echo "${trace_resp}" | tail -n1)
if [[ "${trace_status}" == "200" ]]; then
    info "Trace logging enabled in no-KMS cluster."
    info "TRACE messages will appear in: ${REPO_ROOT}/build/testclusters/runTask-0/logs/runTask.log"
else
    info "WARNING: could not set trace logging (HTTP ${trace_status}) — continuing."
fi

# ---- Phase 4: Run KMS-unavailable tests -------------------------------------

info "=== Phase 4: Run KMS-unavailable tests ==="
bash "${SCRIPT_DIR}/test-pme-kms-unavailable.sh"
KMS_UNAVAIL_EXIT=$?

# ---- result ------------------------------------------------------------------

info "=== Phase 5: Results ==="
if [[ ${TEST_EXIT} -eq 0 && ${KMS_UNAVAIL_EXIT} -eq 0 ]]; then
    info "All PME end-to-end tests PASSED (main + KMS-unavailable)."
    exit 0
else
    [[ ${TEST_EXIT}        -ne 0 ]] && info "Main PME tests FAILED (exit ${TEST_EXIT})."
    [[ ${KMS_UNAVAIL_EXIT} -ne 0 ]] && info "KMS-unavailable tests FAILED (exit ${KMS_UNAVAIL_EXIT})."
    exit 1
fi







