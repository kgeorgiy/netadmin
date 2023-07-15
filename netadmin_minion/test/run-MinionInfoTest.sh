#!/bin/bash
set -euo pipefail

BASE="$(dirname "$0")"
java "$BASE/MinionInfoTest.java" \
    "$BASE/../__keys/netadmin-server.jks" \
    "${1:-localhost}" "${2:-6236}" \
    2>&1 | tee __log
