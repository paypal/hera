CURRENT_GO_VERSION="go1.18.2"
echo "Setting the env"
export BASEPATH=$PATH
export GOPATH=/$CURRENT_GO_VERSION
export GOBIN=/$CURRENT_GO_VERSION/src/bin
mkdir -p $GOBIN
export GOROOT=/usr/local/$CURRENT_GO_VERSION/go
export GO111MODULE="auto"
export GOFLAGS="-count=1"
export PATH=$GOPATH/bin:/usr/local/$CURRENT_GO_VERSION/bin:$BASEPATH
export PATH=/x/opt/pp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH
export PATH=$GOROOT/bin/:/x/opt/pp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/lib
export PKG_CONFIG_PATH=/x/opt/ppopenssl-1.0.1e/lib/pkgconfig
cd $GOPATH/src
