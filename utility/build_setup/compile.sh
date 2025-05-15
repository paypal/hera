#!/bin/bash
docker run -v ./src:/go1.18.2/src  -it artifactory.paypalcorp.com/javadataaccess/occbld /bin/bash /go1.18.2/src/github.com/paypal/hera/utility/build_setup/comp.sh
docker cp bin/mux occ:/x/web/LIVE/occ/mysql/
docker exec occ sudo chown occ:occ /x/web/LIVE/occ/mysql/mux
docker cp bin/watchdog occ:/x/web/LIVE/occ/occwatchdog
docker exec occ sudo chown occ:occ /x/web/LIVE/occ/occwatchdog
docker cp bin/mysqlworker occ:/x/web/LIVE/occ/mysql/
docker exec occ sudo chown occ:occ /x/web/LIVE/occ/mysql/mysqlworker

if [ "$COMPCC" == "true" ]
then
  docker cp bin/oracleworker19c occ:/x/web/LIVE/occ/mysql/oracleworker
  docker exec occ sudo chown occ:occ /x/web/LIVE/occ/mysql/oracleworker
fi
docker exec occ sudo kill -s SIGHUP 1
