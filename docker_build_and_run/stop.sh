#!/bin/bash

export HERA_DB_PASSWORD=""
export HERA_DB_SCHEMA=""
export HERA_DB_USER=""
export HERA_DISABLE_SSL=""
export MYSQL_VERSION=""
export HERA_DB_ROOT_PASSWORD=""
export HERA_TIME_ZONE=""

docker-compose -f MySqlHeraMockHeraBox.yaml down -v --remove-orphans
ps -eaf | grep HeraIntegratedSpringApplication | grep -v grep | awk '{print $2}' | xargs kill -9
