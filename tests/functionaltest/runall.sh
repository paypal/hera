if [ x$GOPATH = x ]
then
  # when running in github actions workflow
  export GOPATH=$PWD/testrun
  mkdir -p testrun/src/github.com/paypal
  ln -s $PWD testrun/src/github.com/paypal/hera
fi
$GOROOT/bin/go install github.com/paypal/hera/worker/mysqlworker
suites="bind_eviction_tests strandedchild_tests coordinator_tests saturation_tests adaptive_queue_tests rac_tests sharding_tests"
finalResult=0
for suite in $suites
do 
  for d in `ls -F $GOPATH/src/github.com/paypal/hera/tests/functionaltest/$suite | grep /$ | sed -e 's,/,,' | egrep -v '(testutil|no_shard_no_error|set_shard_id_wl|reset_shard_id_wl)'`
  do 
      pushd $GOPATH/src/github.com/paypal/hera/tests/functionaltest/$suite/$d
      cp $GOPATH/bin/mysqlworker .
      $GOROOT/bin/go test -c .
      ./$d.test
      rv=$?
      if [ 0 != $rv ]
      then
         echo failing $suite $d
         grep ^ *.log
         finalResult=$rv
#        exit $rv
      fi
      popd
      sleep 10
  done
done
exit $finalResult
