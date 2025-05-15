CURRENT_GO_VERSION="go1.18.2"
echo "Setting the env"
export BASEPATH=$PATH
export GOPATH=/$CURRENT_GO_VERSION
export GOBIN=/$CURRENT_GO_VERSION/src/bin
mkdir -p $GOBIN
mkdir -p /$CURRENT_GO_VERSION/src/reports/
export GOROOT=/usr/local/$CURRENT_GO_VERSION/go
export GO111MODULE="auto"
export GOFLAGS="-count=1"
export PATH=$GOPATH/bin:/usr/local/$CURRENT_GO_VERSION/bin:$BASEPATH
export PATH=/x/opt/pp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH
export PATH=$GOROOT/bin/:/x/opt/pp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/lib
export PKG_CONFIG_PATH=/x/opt/ppopenssl-1.0.1e/lib/pkgconfig
cd $GOPATH/src
cd github.com/paypal/hera/
#go install github.com/axw/gocov/gocov@latest
#go install github.com/matm/gocov-html/cmd/gocov-html@latest
#$GOBIN/gocov test -timeout=20m -v github.com/paypal/hera/tests/$1 | $GOBIN/gocov-html > /$CURRENT_GO_VERSION/src/reports/coverage.html
go install gotest.tools/gotestsum@latest
$GOBIN/gotestsum  --junitfile /$CURRENT_GO_VERSION/src/reports/report.xml  --format standard-verbose github.com/paypal/hera/tests/$1 -test.timeout 99999s
xsltproc /pypl/unit_test_output.xsl /$CURRENT_GO_VERSION/src/reports/report.xml > /$CURRENT_GO_VERSION/src/reports/report.html
#go test -v github.com/paypal/hera/tests/$1 -count=1 -timeout 99999s
