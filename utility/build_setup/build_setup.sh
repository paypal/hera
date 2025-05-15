#!/bin/bash

# Download golang compiler
FILE="./go1.18.2.linux-amd64.tar.gz"
if [ ! -f $FILE ]; then
  echo "Downloading golang compiler ..."
  curl -sSf -O https://artifactory.paypalcorp.com/artifactory/generic-uploads/3rdparty/golang/1.18.2/go1.18.2.linux-amd64.tar.gz
fi

# Download Oracle client library
FILE="./oracle-instant-client-19.17.0.0.tar.gz"
if [ ! -f $FILE ]; then
  echo "Downloading oracle client lib ..."
  curl -sSf -O https://artifactory.paypalcorp.com/artifactory/generic-uploads/3rdparty/oracle/oracle-instant-client-19.17.0.0.tar.gz
fi

docker build -t occbld -f Dockerfile.build  .
