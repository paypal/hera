overall=0
for d in `ls -F tests/unittest | grep /$ | sed -e "s,/,," | egrep -v '(mysql_recycle|log_checker_initdb|testutil|rac_maint|mysql_direct|failover|otel_basic|otel_incorrect_endpoint|otel_sharding|otel_with_skip_cal)'`
do 
    echo ==== $d
    pushd tests/unittest/$d 
    cp /home/runner/go/bin/mysqlworker .
    rm -f *.log 
    $GOROOT/bin/go test -c github.com/paypal/hera/tests/unittest/$d 
    ./$d.test -test.v
    rv=$?
    grep -E '(FAIL|PASS)' -A1 *.log
    if [ 0 != $rv ]
    then
        echo "Retrying" $d
        echo "exit code" $rv 
        ./$d.test -test.v
        rv=$?
        grep -E '(FAIL|PASS)' -A1 *.log
    fi
    if [ 0 != $rv ]
    then
        echo "--- HERA_LOG ---"
        grep ^ hera.log
        echo "--- CAL LOG ---"
        grep ^ cal.log
        popd
        #exit $rv
        overall=1
        continue
    fi
    rm -f *.log 
    popd
done
exit $overall
