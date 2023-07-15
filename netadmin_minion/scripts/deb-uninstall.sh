#!/bin/bash
set -eu

NAME=netadmin-minion

service "$NAME" stop
update-rc.d -f "$NAME" remove
rm -rf "/etc/init.d/$NAME" "/bin/$NAME" "/etc/$NAME"

echo "$NAME uninstalled"
