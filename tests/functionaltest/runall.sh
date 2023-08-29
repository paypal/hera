if [ x$GOPATH = x ]
then
  # when running in github actions workflow
  export GOPATH=$PWD/testrun
  mkdir -p testrun/src/github.com/paypal
  ln -s $PWD testrun/src/github.com/paypal/hera
fi
$GOROOT/bin/go install github.com/paypal/hera/worker/mysqlworker
$GOROOT/bin/go install github.com/paypal/hera/watchdog
$GOROOT/bin/go install github.com/paypal/hera/mux
ls -l $GOPATH/bin

cat << EOF > shortRun
sharding_tests/set_shard_id
sharding_whitelist_tests/no_shard_no_error
sharding_whitelist_tests/set_shard_id_wl
bind_eviction_tests/bind_eviction_disable
EOF

basedir=$GOPATH/src/github.com/paypal/hera/tests/functionaltest/
find $basedir -name main_test.go  | sed -e "s,^$basedir,,;s,/main_test.go,," > toRun

# suites="bind_eviction_tests strandedchild_tests coordinator_tests saturation_tests adaptive_queue_tests rac_tests sharding_tests"
# ls -F $GOPATH/src/github.com/paypal/hera/tests/functionaltest/$suite | grep /$ | sed -e 's,/,,' | egrep -v '(testutil|
#     no_shard_no_error|set_shard_id_wl|reset_shard_id_wl)' | sed -e "s,^,$suite/," >> toRun

finalResult=0
for pathD in `cat toRun` # shortRun 
do 
    pushd $GOPATH/src/github.com/paypal/hera/tests/functionaltest/$pathD
    d=`basename $pathD`
    ln $GOPATH/bin/mysqlworker .

    if [ -f ../setup-mysql.sql -a ! -f ../setup-mysql.sql.out ]
    then
        cat ../setup-mysql.sql | mysql -h127.0.0.1 -uroot -p1-testDb heratestdb | tee ../setup-mysql.sql.out
    elif [ -f ../setup-mysql.sql.out ]
    then 
        ls -l ../setup-mysql.sql.out
    else
        echo no common setup-mysql.sql $pathD
    fi
    if [ -f setup-mysql.sql ]
    then
        cat setup-mysql.sql | mysql -h127.0.0.1 -uroot -p1-testDb heratestdb
    fi

    $GOROOT/bin/go test -c .
    ./$d.test -test.v 2>&1 | tee std.log
    egrep -n '^--- (PASS|[^:]*):' std.log
    if ! grep -q '^--- PASS:' std.log
    then
        echo failing $pathD will retry
        pkill watchdog
        pkill mux 
        pkill mysqlworker
        if [ -f setup-mysql.sql ]
        then
            cat setup-mysql.sql | mysql -h127.0.0.1 -uroot -p1-testDb heratestdb
        fi
        mv std{,1}.log
        ./$d.test -test.v 2>&1 | tee std.log
        egrep -n '^--- (PASS|[^:]*):' std.log
        if ! grep -q '^--- PASS:' std.log
        then
            sleep 0.01
            tail -n111 *.log
            echo failing $pathD on retry ====
            finalResult=1
        fi
    fi
    pkill watchdog
    pkill mux 
    pkill mysqlworker
    popd
done
exit $finalResult
