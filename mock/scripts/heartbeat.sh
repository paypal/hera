#!/usr/bin/env bash
sleep 30;
curl http://localhost:8000/running;
while true
do
    curl http://localhost:8000/heartbeat;
    sleep 30;
done

