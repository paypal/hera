#!/bin/bash
echo "Starting Juno services before executing tests..."
echo "Generating secrets..."
./tests/functionaltest/caching_tests/manifest/config/secrets/gensecrets.sh
echo "Copy certs for SSL connectivity..."
cp ./tests/functionaltest/caching_tests/manifest/config/secrets/server.crt .
cp ./tests/functionaltest/caching_tests/manifest/config/secrets/server.pem .
cp ./tests/functionaltest/caching_tests/manifest/config/secrets/ca.crt .
./tests/functionaltest/caching_tests/start-juno.sh
overall=0
export CACHE_HOST="localhost"
export CACHE_CERTS_PATH=""
for d in `ls -F tests/functionaltest/caching_tests | grep /$ | sed -e "s,/,,"`
do 
    echo ==== $d
    # pushd tests/unittest/$d 
    # cp /home/runner/go/bin/mysqlworker .
    rm -f *.log 
    $GOROOT/bin/go test -c github.com/paypal/hera/tests/functionaltest/caching_tests/$d
    ./$d.test -test.v
    rv=$?
    grep -E '(FAIL|PASS)' -A1 *.log
    if [ 0 != $rv ]
    then
        echo "Retrying" $d
        echo "exit code" $rv 
        ./$d.test -test.v
        rv=$?
        grep -B5 -A5 -E '(FAIL|PASS)' -A1 *.log
    fi
    if [ 0 != $rv ]
    then
        #grep ^ *.log
        # popd
        cp hera.log hera.log.$d
        #exit $rv
        overall=1
        continue
    fi
    rm -f *.log 
    # popd
done
exit $overall
echo "cleanup Juno certs"
find ./tests/functionaltest/caching_tests/manifest/config/secrets -maxdepth 1 -type f ! -name 'gensecrets.sh' ! -name 'readme.md' -exec rm -v {} +
echo "Shutdown Juno services...."
./tests/functionaltest/caching_tests/shutdown-juno.sh
echo "Cleanup certs..."
rm server.crt
rm server.pem
rm ca.crt