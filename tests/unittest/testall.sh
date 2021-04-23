for d in `ls -F $GOPATH/src/github.com/paypal/hera/tests/unittest | grep /$ | egrep -v '(testutil|rac_maint|mysql_direct)'`
do 
    echo ==== $d
    cd $GOPATH/src/github.com/paypal/hera/tests/unittest/$d 
    rm -f *.log 
    $GOROOT/bin/go test -c github.com/paypal/hera/tests/unittest/$n 
    ./$n.test 
    rv=$?
    grep -E '(FAIL|PASS)' -A1 *.log
    if [ 0 != $rv ]
    then
        grep ^ *.log
        exit $rv
    fi
done
