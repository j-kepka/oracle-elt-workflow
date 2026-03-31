#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ORACLE_CONTAINER_NAME="${ORACLE_CONTAINER_NAME:-oracle-free}"
ORACLE_VOLUME_NAME="${ORACLE_VOLUME_NAME:-oracle-data}"
ORACLE_IMAGE="${ORACLE_IMAGE:-gvenzl/oracle-free@sha256:62aad247879f5d4ca4a37ecc068ef6a5feb9e9bea789501b6a82d4814d14bbb3}"
ORACLE_PASSWORD="${ORACLE_PASSWORD:-}"
ORACLE_TZ="${ORACLE_TZ:-Europe/Berlin}"

if [[ -z "${ORACLE_PASSWORD}" ]]; then
  echo "Set ORACLE_PASSWORD before starting the container." >&2
  echo "Example: ORACLE_PASSWORD='change-me' ./ops/00_start_oracle_container.sh" >&2
  exit 1
fi

docker volume create "${ORACLE_VOLUME_NAME}" >/dev/null
docker rm -f "${ORACLE_CONTAINER_NAME}" >/dev/null 2>&1 || true

docker run -d --name "${ORACLE_CONTAINER_NAME}" \
  -p 127.0.0.1:1521:1521 \
  -p 127.0.0.1:5500:5500 \
  -e ORACLE_PASSWORD="${ORACLE_PASSWORD}" \
  -e TZ="${ORACLE_TZ}" \
  -e ORA_SDTZ="${ORACLE_TZ}" \
  -v "${ORACLE_VOLUME_NAME}:/opt/oracle/oradata" \
  -v "${REPO_ROOT}/extdata:/opt/oracle/extdata" \
  -v "${REPO_ROOT}:/workspace" \
  "${ORACLE_IMAGE}"

echo "Started ${ORACLE_CONTAINER_NAME} with timezone ${ORACLE_TZ}."
echo "Current startup config uses TZ=${ORACLE_TZ} and ORA_SDTZ=${ORACLE_TZ}."
