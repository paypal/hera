for d in `ls -F $GOPATH/src/github.com/paypal/hera/tests/unittest | grep /$ | egrep -v '(testutil|rac_maint|mysql_direct)'`
do 
    $GOROOT/bin/go test github.com/paypal/hera/tests/unittest/$d 
done
