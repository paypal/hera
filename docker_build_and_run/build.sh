#!/bin/bash

##################################################################################################
# This script will build hera and heramock.
# Then it will create docker compose with proper setup on mysql, hera and heramock
# Start and stop scripts inside will help in starting all of them as one environment
##################################################################################################
BUILD_HERA=${BUILD_HERA:-true}
BUILD_HERA_MOCK=${BUILD_HERA_MOCK:-true}

# Copy the hera source code to compile hera and create hera container
if [ "$BUILD_HERA" = true ] ; then
  echo "Building Hera Code ..."
  mkdir -p src/github.com/paypal/hera
  rsync -av --exclude mock --exclude tests --exclude docker_build_and_run .. src/github.com/paypal/hera
  docker build -f HeraDockerfile -t hera-oss .
fi

if [ "$BUILD_HERA_MOCK" = true ] ; then
  echo "Building HeraMock Code ..."
  # build the heramock and create the container
  cd ../mock
  cd mockClient/java && mvn clean -DskipTests install ; cd ../../
  docker build -f HeraMockDockerfile -t hera-mock .
fi
