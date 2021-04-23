for d in `ls -F $GOPATH/src/github.com/paypal/hera/tests/unittest | grep /$ | egrep -v '(testutil|rac_maint|mysql_direct)'`
do 
    echo ==== $d
    pushd tests/unittest/$d 
    rm -f *.log 
    $GOROOT/bin/go test -c github.com/paypal/hera/tests/unittest/$d 
    ./$d.test 
    rv=$?
    grep -E '(FAIL|PASS)' -A1 *.log
    if [ 0 != $rv ]
    then
        grep ^ *.log
        popd
        exit $rv
    fi
    popd
done
