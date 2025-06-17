#!/bin/bash
set -e

# Usage check
if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <topic> <config> <profile> <host> <port>"
    exit 1
fi

topic="$1"
config="$2"
profile="$3"
host="$4"
port="$5"

# Create random pipes in /tmp
p1=$(mktemp -u /tmp/mqtt2nc.XXXX)
p2=$(mktemp -u /tmp/nc2mqtt.XXXX)
mkfifo "$p1" "$p2"

# Cleanup on exit
trap "rm -f $p1 $p2; kill 0" EXIT

# Bridge processes
cat "$p2" | python3 mqtt_pipe.py listen "$topic" "$config" "$profile" > "$p1" &
cat "$p1" | nc "$host" "$port" > "$p2" &
wait
