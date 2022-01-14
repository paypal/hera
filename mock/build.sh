#!/bin/bash

echo "Building HeraMock Code ..."
# build the heramock and create the container
#cd ../mock
#cd mockClient/java && mvn clean -DskipTests install ; cd ../../
BASE_IMAGE=${BASE_IMAGE:-"openresty/openresty"}
export BASE_IMAGE

docker build --build-arg BASE_IMAGE=${BASE_IMAGE} -f HeraMockDockerfile -t hera-mock .