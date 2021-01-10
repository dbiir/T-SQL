#!/bin/bash

GPFDIST_PID=gpfdist.pid

export GPFDIST_WATCHDOG_TIMER=3
gpfdist &
PID=$!

sleep 5
ps -p $PID
if [ $? -eq 0 ]; then
# gpfdist not aborted by watchdog, failed
kill -9 $PID
wait $PID
echo "gpfdist still running, failed"
exit 1
fi

# gpfdist aborted by watchdog, passed
wait $PID
echo "gpfdist stop by watchdog, success"
exit 0
