set -e # exit on cmd return 
if [ x$GOPATH = x ]
then
  # when running in github actions workflow
  export GOPATH=$PWD/testrun
  mkdir -p testrun/src/github.com/paypal
  ln -s $PWD testrun/src/github.com/paypal/hera
fi

sudo apt install libboost-regex-dev -y
wget -nv https://download.oracle.com/otn_software/linux/instantclient/1919000/instantclient-basiclite-linux.x64-19.19.0.0.0dbru.zip https://download.oracle.com/otn_software/linux/instantclient/1920000/instantclient-sqlplus-linux.x64-19.20.0.0.0dbru.zip
curl -O https://download.oracle.com/otn_software/linux/instantclient/1919000/instantclient-sdk-linux.x64-19.19.0.0.0dbru.zip
echo 409b867f76c701ccba47f9278363b204137fc92444c317b36b60da35669453a99bd02a3c84b1b9b92c54fd94929a0eff  instantclient-sqlplus-linux.x64-19.20.0.0.0dbru.zip >> SHA384
echo bb68094a12e754fc633874e8c2b4c4d38a45a65a5a536195d628d968fca72d7a5006a62a7b1fdd92a29134a06605d2b4  instantclient-basiclite-linux.x64-19.19.0.0.0dbru.zip >> SHA384
echo 5999f2333a9b73426c7af589ab13480f015c2cbd82bb395c7347ade37cc7040a833a398e9ce947ae2781365bd3a2e371  instantclient-sdk-linux.x64-19.19.0.0.0dbru.zip >> SHA384
sha384sum -c SHA384
pubdir=$PWD

pushd /opt
mkdir instantclient_19
ln -s instantclient_19 instantclient_19_17
ln -s instantclient_19 instantclient_19_19
ln -s instantclient_19 instantclient_19_20
unzip $pubdir/instantclient-basiclite-linux.x64-19.19.0.0.0dbru.zip
unzip $pubdir/instantclient-sdk-linux.x64-19.19.0.0.0dbru.zip
unzip $pubdir/instantclient-sqlplus-linux.x64-19.20.0.0.0dbru.zip
popd

export ORACLE_HOME=/opt/instantclient_19
mkdir -p $ORACLE_HOME/network/admin
echo 'TEST3=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(Host=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=XEPDB1)(FAILOVER_MODE=(TYPE=SESSION)(METHOD=BASIC)(RETRIES=1000)(DELAY=5)))))' > $ORACLE_HOME/network/admin/tnsnames.ora
find $ORACLE_HOME/network -ls
export TWO_TASK=TEST3
export TNS_ADMIN=./
export OPS_CFG_FILE=occ.cdb
export username=system
export password=1.2.8MomOfferExpand
if [ x$LD_LIBRARY_PATH = x ]
then
    export LD_LIBRARY_PATH=$ORACLE_HOME
else
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME
fi

# make oracle worker
pushd worker/cppworker/worker
make -f ../build/makefile19 -j 3
mkdir -p $GOPATH/bin
cp -v oracleworker $GOPATH/bin/
popd


# run test with oracle
overall=0
for d in `ls $GOPATH/src/github.com/paypal/hera/tests/unittest3 | grep -vE '(testall.sh)'`
do
    pushd $GOPATH/src/github.com/paypal/hera/tests/unittest3/$d
    if [ -f setup.sql ]
    then
        cat setup.sql |$ORACLE_HOME/sqlplus $username/$password@$TWO_TASK
    fi
    cp -v $GOPATH/bin/oracleworker .
    $GOROOT/bin/go test -c .
    ./$d.test -test.v | tee /dev/null
    rv=$?
    if [ 0 != $rv ]
    then
        echo $d failing $d
        grep ^ *.log
        overall=$rv
    fi
    tail *.log
    echo $d test done $rv
    grep -E '(FAIL([^O]|$)|PASS)' -A1 *.log
    popd
done
exit $overall
