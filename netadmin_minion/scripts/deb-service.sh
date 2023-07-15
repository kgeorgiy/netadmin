#!/bin/sh
### BEGIN INIT INFO
# Provides:          netadmin-minion
# Required-Start:    $all
# Required-Stop:     $all
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Description:       Executes NetAdmin requests
### END INIT INFO

NAME=netadmin-minion
RUNAS=root

SCRIPT="/bin/$NAME"
CONFIG="/etc/$NAME/netadmin-minion.yaml"
PIDFILE="/var/run/$NAME.pid"

log() {
    echo $1 >&2
}

start() {
    start-stop-daemon --start --user "$RUNAS" --pidfile "$PIDFILE" --name "$NAME" \
        --chuid "$RUNAS" --background --startas "$SCRIPT" -- --config "$CONFIG" \
        && log "$NAME started"
}

stop() {
    start-stop-daemon --stop --user "$RUNAS" --pidfile "$PIDFILE" --name "$NAME" \
        && log "$NAME stopped"
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    retart)
        stop
        start
        ;;
    *)
        log "Usage: $0 {start|stop|restart}"
        ;;
esac
