#!/bin/bash
  
HERA_DB_ROOT_PASSWORD=${HERA_DB_ROOT_PASSWORD:=62-AntHill}
MYSQL_VERSION=${MYSQL_VERSION:-latest}
HERA_DB=${HERA_DB:-testschema}

export HERA_DB_ROOT_PASSWORD
export MYSQL_VERSION
export HERA_DB

docker-compose -f MySqlHeraMockHeraBox.yaml down -v --remove-orphans
