#!/bin/bash

##################################################################################################
# This script will build hera and heramock.
# Then it will create docker compose with proper setup on mysql, hera and heramock
# Start and stop scripts inside will help in starting all of them as one environment
##################################################################################################

# Copy the hera source code to compile hera and create hera container
mkdir -p src/github.com/paypal/hera
rsync -av --exclude mock --exclude tests .. src/github.com/paypal/hera
docker build -f HeraDockerfile -t hera-oss .

# build the heramock and create the container
cd mockClient/java && mvn clean -DskipTests install ; cd -
docker build -f HeraMockDockerfile -t hera-mock .
