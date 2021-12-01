suites="bind_eviction_tests strandedchild_tests coordinator_tests saturation_tests adaptive_queue_tests rac_tests sharding_tests"
for suite in $suites
do 
  for d in `ls -F $GOPATH/src/github.com/paypal/hera/tests/functionaltest/$suite | grep /$ | egrep -v '(testutil|no_shard_no_error|set_shard_id_wl|reset_shard_id_wl)'`
  do 
      $GOROOT/bin/go test github.com/paypal/hera/tests/functionaltest/$suite/$d 
      rv=$?
      if [ 0 != $rv ]
      then
         echo failing
#        exit $rv
      fi
      sleep 10
  done
done
