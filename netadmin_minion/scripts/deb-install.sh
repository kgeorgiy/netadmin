#!/bin/bash
set -eu

NAME=netadmin-minion
SCRIPT=/etc/init.d/$NAME
BASE="$(dirname "${BASH_SOURCE[0]}")/.."

cp "$BASE/scripts/deb-service.sh" "$SCRIPT"
cp "$BASE/../target/release/netadmin-minion" "/bin/$NAME"

mkdir -p "/etc/$NAME"
cp "$BASE/resources/netadmin-minion.yaml" \
   "$BASE/__keys/netadmin-server.crt" \
   "$BASE/__keys/netadmin-minion.crt" \
   "$BASE/__keys/netadmin-minion.key" \
   "/etc/$NAME/"

chown root:root "$SCRIPT" "/bin/$NAME"
chmod +x "$SCRIPT" "/bin/$NAME"

update-rc.d -f "$NAME" defaults

echo "$NAME" installed
