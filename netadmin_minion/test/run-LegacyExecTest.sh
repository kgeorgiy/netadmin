#!/bin/bash
set -euo pipefail

BASE="$(dirname "$0")"
scala "$BASE/LegacyExecTest.scala" \
    "$BASE/../__keys/netadmin-server.jks" \
    localhost 12345 \
    "echo hello world" \
    2>&1 | tee test/__log
