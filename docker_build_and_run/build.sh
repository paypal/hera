#!/bin/bash

##################################################################################################
# This script will build hera and heramock.
# Then it will create docker compose with proper setup on mysql, hera and heramock
# Start and stop scripts inside will help in starting all of them as one environment
##################################################################################################
BUILD_HERA=${BUILD_HERA:-true}
BUILD_HERA_MOCK=${BUILD_HERA_MOCK:-true}
BASE_IMAGE=${BASE_IMAGE:-"openresty/openresty"}
BUILD_SAMPLE_APP=${BUILD_SAMPLE_APP:-true}
export BASE_IMAGE

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
  docker build --build-arg BASE_IMAGE=${BASE_IMAGE} -f HeraMockDockerfile -t hera-mock .
fi

if [ "$BUILD_SAMPLE_APP" = true ] ; then
  echo "Building HeraMock Code ..."
  # build the heramock and create the container
  cd ../sample_hera_based_app/
  mvn clean install -DskipTests;
fi