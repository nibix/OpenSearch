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
CLUSTER_PID_FILE="${SCRIPT_DIR}/.cluster.pid"
CLUSTER_LOG_FILE="${SCRIPT_DIR}/.cluster.log"

info()  { echo "[teardown] $*"; }
error() { echo "[teardown] ERROR: $*" >&2; }

if [[ ! -f "${CLUSTER_PID_FILE}" ]]; then
    info "No .cluster.pid file found — nothing to stop."
    exit 0
fi

pid=$(cat "${CLUSTER_PID_FILE}")

if ! kill -0 "${pid}" 2>/dev/null; then
    info "Process ${pid} is already gone."
    rm -f "${CLUSTER_PID_FILE}"
    exit 0
fi

info "Stopping Gradle run process (PID ${pid}) and child OpenSearch processes ..."

# Kill the entire process group so child JVM processes are also terminated.
pgid=$(ps -o pgid= -p "${pid}" | tr -d ' ')
if [[ -n "${pgid}" && "${pgid}" != "0" ]]; then
    kill -- "-${pgid}" 2>/dev/null || kill "${pid}" 2>/dev/null || true
else
    kill "${pid}" 2>/dev/null || true
fi

# Wait up to 15 seconds for the process to exit.
timeout=15
elapsed=0
while kill -0 "${pid}" 2>/dev/null; do
    sleep 1
    elapsed=$((elapsed + 1))
    if [[ ${elapsed} -ge ${timeout} ]]; then
        info "Process did not stop cleanly — sending SIGKILL."
        kill -9 "${pid}" 2>/dev/null || true
        break
    fi
done

rm -f "${CLUSTER_PID_FILE}"
info "Cluster stopped."

if [[ -f "${CLUSTER_LOG_FILE}" ]]; then
    info "Log file retained at: ${CLUSTER_LOG_FILE}"
fi

