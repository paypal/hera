#!/bin/bash

export HERA_DB_PASSWORD=""
export HERA_DB_SCHEMA=""
export HERA_DB_USER=""
export HERA_ENABLE_SSL=""
export MYSQL_VERSION=""
export HERA_DB_ROOT_PASSWORD=""

docker-compose -f MySqlHeraMockHeraBox.yaml down -v --remove-orphans
