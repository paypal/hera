overall=0
for d in `ls -F tests/unittest | grep /$ | sed -e "s,/,," | egrep -v '(mysql_recycle|log_checker_initdb|testutil|rac_maint|mysql_direct|failover)'`
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
        #grep ^ *.log
        echo "Retry failed, exit code" $rv
        echo "======================================== $d Tests Failed. Start of logs ======================================="
        log_file="hera.log"
        if [ -f "$log_file" ]; then
            cat "$log_file"
        else
            echo "Log file: ${log_file} does not exist."
        fi
        log_file="occ.log"
        if [ -f "$log_file" ]; then
            cat "$log_file"
        else
            echo "Log file: ${log_file} does not exist."
        fi
        echo "======================================== End of logs for $d test===================================================="
        popd
        overall=1
        continue
    fi
    rm -f *.log 
    popd
done
exit $overall
