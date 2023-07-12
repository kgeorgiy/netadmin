#!/bin/bash
set -euo pipefail

pushd .. > /dev/nul
java test/MinionInfoTest.java 2>&1 | tee test/__log
popd > /dev/nul
