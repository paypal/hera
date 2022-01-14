#!/bin/bash

if [ $# -gt 0 ] 
then
  echo "**************************************************"
  echo "This script will build hera docker image and run it along with mysql."
  echo "********************==========********************"
  echo "Options that can be passed before starting HeraBox"
  echo "Note: All options has to be set thru environment Variables"
  echo "********************==========********************"
  echo "1. MYSQL_VERSION - Default is 'latest'"
  echo "2. HERA_DB_ROOT_PASSWORD - Default is 'UseHeraToScaleDB'"
  echo "3. HERA_DB_SCHEMA - Default is 'testschema'"
  echo "4. HERA_DB_USER - Default is 'herauser'"
  echo "5. HERA_DB_PASSWORD - Default is 'herapassword'"
  echo "6. HERA_DISABLE_SSL - Default is 'false'"
  echo "7. HERA_RUN_WITH_MOCK - Default is 'true'"
  echo "8. HERA_TIME_ZONE - Default is 'America/Los_Angeles'"
  echo "********************==========********************"
  exit
fi

HERA_DB_ROOT_PASSWORD=${HERA_DB_ROOT_PASSWORD:-UseHeraToScaleDB}
MYSQL_VERSION=${MYSQL_VERSION:-latest}
HERA_DB_SCHEMA=${HERA_DB_SCHEMA:-testschema}
HERA_DB_USER=${HERA_DB_USER:-herauser}
HERA_DB_PASSWORD=${HERA_DB_PASSWORD:-herapassword}
HERA_DISABLE_SSL=${HERA_DISABLE_SSL:-false}
HERA_RUN_WITH_MOCK=${HERA_RUN_WITH_MOCK:-true}
HERA_TIME_ZONE="America/Los_Angeles"

export HERA_DB_ROOT_PASSWORD
export MYSQL_VERSION
export HERA_DB_SCHEMA
export HERA_DB_USER
export HERA_DB_PASSWORD
export HERA_DISABLE_SSL
export HERA_RUN_WITH_MOCK
export HERA_TIME_ZONE

echo "Starting MySQL and hera ..."
echo "*****************************"
echo "Settings: "
echo "MYSQL VERIONS: "$MYSQL_VERSION", MOCK_ENABLED: "$HERA_RUN_WITH_MOCK
echo "DB USER: "$HERA_DB_USER", SCHEMA: "$HERA_DB_SCHEMA
echo "DISABLE SSL: "$HERA_DISABLE_SSL", TIMEZONE: "$HERA_TIME_ZONE
echo "*****************************"

if [ "$HERA_RUN_WITH_MOCK" = true ] ; then
  docker-compose -f MySqlHeraMockHeraBox.yaml up -d --remove-orphans
else
  docker-compose -f MySqlHeraBox.yaml up -d --remove-orphans
fi

echo "Running initial set of queries ..."
case "${unameOut}" in
    MING*)    winpty docker exec -it hera_mysql mysql -u root -p$HERA_DB_ROOT_PASSWORD  -e "Use $HERA_DB_SCHEMA; $(cat ./initialize.sql)";;
    *)        docker exec -it hera_mysql mysql -u root -p$HERA_DB_ROOT_PASSWORD  -e "Use $HERA_DB_SCHEMA; $(cat ./initialize.sql)"
esac
