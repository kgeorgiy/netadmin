#!/bin/bash
set -euo pipefail

pushd .. > /dev/nul
scala -cp test test/LegacyExecTest.scala \
    localhost 12345 \
    "echo hello world" \
    2>&1 | tee test/__log
popd > /dev/nul
