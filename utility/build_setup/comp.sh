source /pypl/env.sh
cd $GOPATH/src
cd github.com/paypal/hera/
go mod tidy
mkdir $GOROOT/src/utility
cp -af ../../../utility/* $GOROOT/src/utility/
go get github.com/youmark/pkcs8
rm -rf /go1.18.2/pkg/mod/github.com/youmark/pkcs8\@v0.0.0-20240726163527-a2c0da244d78/*
cp ../../youmark/pkcs8/* /go1.18.2/pkg/mod/github.com/youmark/pkcs8\@v0.0.0-20240726163527-a2c0da244d78/
echo "compiling..."


if [ "$COMPILE_MUX" == "true" ]
then
   $GOROOT/bin/go install github.com/paypal/hera/mux
else
    echo "If you want to complile mux set env COMPILE_MUX to true"
fi

if [ "$COMPILE_WATCHDOG" == "true" ]
then
   $GOROOT/bin/go install github.com/paypal/hera/watchdog
else
    echo "If you want to complile watchdog set env COMPILE_WATCHDOG to true"
fi

if [ "$COMPILE_WORKER" == "true" ]
then
   $GOROOT/bin/go install github.com/paypal/hera/worker/mysqlworker
   $GOROOT/bin/go install github.com/paypal/hera/worker/postgresworker
else
    echo "If you want to complile worker set env COMPILE_WORKER to true"
fi


if [ "$COMPCC" == "true" ]
then
    echo "compiling c++ worker"
    cd $GOPATH/src/github.com/paypal/hera/worker/cppworker/worker
    make -B -f ../build/makefile19
    cp oracleworker $GOBIN/oracleworker19c
else
    echo "If you want to complile c++ worker set env COMPCC to true"
fi
