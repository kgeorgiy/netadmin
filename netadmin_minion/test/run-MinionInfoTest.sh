#!/bin/bash
set -euo pipefail

BASE="$(dirname "$0")"
java "$BASE/MinionInfoTest.java" \
    "$BASE/../__keys/netadmin-server.jks" \
    2>&1 | tee __log
