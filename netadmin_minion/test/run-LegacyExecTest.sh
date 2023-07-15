#!/bin/bash
set -euo pipefail

BASE="$(dirname "$0")"
scala "$BASE/LegacyExecTest.scala" \
    "$BASE/../__keys/netadmin-server.jks" \
    "${1:-localhost}" "${2:-12345}" \
    "${3:-echo hello world}" \
    2>&1 | tee __log
