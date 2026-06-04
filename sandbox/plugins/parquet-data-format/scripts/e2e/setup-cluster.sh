#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_PID_FILE="${SCRIPT_DIR}/.cluster.pid"
CLUSTER_LOG_FILE="${SCRIPT_DIR}/.cluster.log"

info()  { echo "[setup] $*"; }
error() { echo "[setup] ERROR: $*" >&2; }

wait_for_cluster() {
    local url="http://localhost:9200"
    local timeout=120
    local elapsed=0
    info "Waiting for cluster at ${url} (timeout ${timeout}s) ..."
    while ! curl -sf "${url}/_cluster/health" -o /dev/null 2>/dev/null; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [[ ${elapsed} -ge ${timeout} ]]; then
            error "Cluster did not become available within ${timeout}s."
            error "Check ${CLUSTER_LOG_FILE} for details."
            exit 1
        fi
    done
    info "Cluster is up."
}

# ---- parse args --------------------------------------------------------------

SKIP_BUILD=false
for arg in "$@"; do
    case "${arg}" in
        --skip-build) SKIP_BUILD=true ;;
        *) error "Unknown argument: ${arg}"; exit 1 ;;
    esac
done

# ---- sanity checks -----------------------------------------------------------

if [[ -f "${CLUSTER_PID_FILE}" ]]; then
    existing_pid=$(cat "${CLUSTER_PID_FILE}")
    if kill -0 "${existing_pid}" 2>/dev/null; then
        info "Cluster already running (PID ${existing_pid}). Use teardown.sh first."
        exit 0
    else
        info "Stale PID file found — removing."
        rm -f "${CLUSTER_PID_FILE}"
    fi
fi

if ! command -v java &>/dev/null && [[ -z "${JAVA_HOME:-}" ]]; then
    error "JAVA_HOME is not set and 'java' is not on PATH."
    exit 1
fi

# ---- build -------------------------------------------------------------------

cd "${REPO_ROOT}"

GRADLEW="${REPO_ROOT}/gradlew"
if [[ ! -x "${GRADLEW}" ]]; then
    error "gradlew not found or not executable at ${GRADLEW}"
    exit 1
fi

if [[ "${SKIP_BUILD}" == false ]]; then
    info "Building native Rust library ..."
    "${GRADLEW}" -Dsandbox.enabled=true ':sandbox:libs:dataformat-native:buildRustLibrary' || { error "Rust build failed"; exit 1; }

    info "Building parquet-data-format plugin ..."
    "${GRADLEW}" -Dsandbox.enabled=true ':sandbox:plugins:parquet-data-format:assemble' -x test || { error "parquet-data-format build failed"; exit 1; }

    info "Building mock-pme-kms plugin ..."
    "${GRADLEW}" -Dsandbox.enabled=true ':sandbox:plugins:mock-pme-kms:assemble' -x test || { error "mock-pme-kms build failed"; exit 1; }
else
    info "Skipping build (--skip-build)."
fi

# ---- start cluster -----------------------------------------------------------

info "Starting OpenSearch cluster with parquet-data-format + mock-pme-kms plugins ..."
# -Dtests.opensearch.logger.* is handled by RunTask.java: each property with the
# prefix "tests.opensearch." has the prefix stripped and is written as a key=value
# entry into opensearch.yml via node.setting().  Writing logger.X=TRACE into
# opensearch.yml is the only reliable way to get TRACE-level output from plugin
# code, because the dynamic PUT /_cluster/settings approach sets the level AFTER
# log4j2 has already initialized, and does not always propagate to loggers that
# were created in plugin classloaders before the setting was applied.

"${GRADLEW}" \
    -Dsandbox.enabled=true \
    -PinstalledPlugins='["parquet-data-format", "mock-pme-kms"]' \
    -Dtests.opensearch.logger.org.opensearch.parquet.encryption=TRACE \
    -Dtests.opensearch.logger.org.opensearch.be.datafusion=TRACE \
    run \
    > "${CLUSTER_LOG_FILE}" 2>&1 &

GRADLE_PID=$!
echo "${GRADLE_PID}" > "${CLUSTER_PID_FILE}"
info "Gradle process PID: ${GRADLE_PID} (Gradle output: ${CLUSTER_LOG_FILE})"
info "OpenSearch server log: ${REPO_ROOT}/build/testclusters/runTask-0/logs/runTask.log"

wait_for_cluster

# ---- enable TRACE logging for PME packages ----------------------------------
# OpenSearch supports dynamic logger configuration via the cluster settings API.
# Setting logger.<package>=TRACE causes log4j2 to emit TRACE-level messages for
# that package without requiring a node restart or a custom log4j2.properties.
# This ensures that all encrypted data-access flow messages from
#   org.opensearch.parquet.encryption  (PmeKeyfileManager, PmeDataKeyCache, PmeContext)
#   org.opensearch.be.datafusion       (DatafusionReaderManager)
# appear in the cluster log during e2e tests.

info "Enabling TRACE logging for PME packages via cluster settings API ..."
trace_resp=$(curl -s -w "\n%{http_code}" -X PUT "http://localhost:9200/_cluster/settings" \
    -H "Content-Type: application/json" \
    -d '{
      "transient": {
        "logger.org.opensearch.parquet.encryption": "TRACE",
        "logger.org.opensearch.be.datafusion": "TRACE"
      }
    }')
trace_status=$(echo "${trace_resp}" | tail -n1)
trace_body=$(echo "${trace_resp}" | sed '$d')
if [[ "${trace_status}" == "200" ]]; then
    info "Trace logging enabled (HTTP 200). Response: ${trace_body}"
    info "TRACE messages will appear in: ${REPO_ROOT}/build/testclusters/runTask-0/logs/runTask.log"
else
    info "WARNING: could not set trace logging (HTTP ${trace_status}): ${trace_body} — continuing."
fi

info "Cluster ready. Run test-pme.sh to execute end-to-end tests."

