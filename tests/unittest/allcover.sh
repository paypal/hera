tarball=go1.20.7.linux-amd64.tar.gz
wget -nv https://go.dev/dl/$tarball
echo f0a87f1bcae91c4b69f8dc2bc6d7e6bfcd7524fceec130af525058c0c17b1b44  $tarball | sha256sum -c
if [ x$? != x0 ]
then
    echo bad sha256sum
    exit 1
fi
tar zxf $tarball

export GOROOT=$PWD/go
export GOPATH=/home/runner/go
mkdir -p $GOPATH/src/github.com/paypal
ln -s $PWD $GOPATH/src/github.com/paypal/hera

rm -rf $GOPATH/allcover{,2}
mkdir $GOPATH/allcover

$GOROOT/bin/go install -cover github.com/paypal/hera/worker/mysqlworker

overall=0
for d in `ls -F tests/unittest | grep /$ | sed -e "s,/,," | egrep -v '(mysql_recycle|log_checker_initdb|testutil|rac_maint|mysql_direct|failover|otel_basic|otel_incorrect_endpoint|otel_sharding|otel_with_skip_cal)'`
do 
    echo ==== $d
    cd tests/unittest/$d 
    cp $GOPATH/bin/mysqlworker .
    rm -f *.log 

    $GOROOT/bin/go run ../testutil/regen rewrite tests/unittest/$d
    $GOROOT/bin/go build -cover github.com/paypal/hera/tests/unittest/$d
    mkdir integcov
    GOCOVERDIR=integcov ./$d
    rv=$?
    echo rv rv $rv for test $d under integration coverage run
    $GOROOT/bin/go tool covdata percent -i=integcov
    mkdir $GOPATH/allcover2
    $GOROOT/bin/go tool covdata merge -i=integcov,$GOPATH/allcover -o $GOPATH/allcover2
    rm -rf $GOPATH/allcover
    mv $GOPATH/allcover{2,}

    rm -f *.log 
    cd ../../..
done
$GOROOT/bin/go tool covdata func -i=$GOPATH/allcover
$GOROOT/bin/go tool covdata percent -i=$GOPATH/allcover
$GOROOT/bin/go tool covdata textfmt -i=$GOPATH/allcover -o $GOPATH/allcover.out
$GOROOT/bin/go tool cover -html=$GOPATH/allcover.out -o $GOPATH/allcover.htm
exit $overall
