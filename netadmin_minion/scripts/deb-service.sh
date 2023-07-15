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

running() {
    [ -f $PIDFILE ] && kill -0 $(cat "$PIDFILE") 2> /dev/null
}

log() {
    echo $1 >&2
}

start() {
    if running ; then
        log "$NAME already running"
        return 1
    fi
    log "Starting $NAME"
    su -c "$SCRIPT --config $CONFIG" $RUNAS && log "$NAME started"
}

stop() {
    if ! running ; then
        log "$NAME not running"
        return 1
    fi
    log "Stopping $NAME"
    kill -15 $(cat "$PIDFILE") && rm -f "$PIDFILE"
    log "$NAME stopped"
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
