package util

import (
	"context"
	"crypto/x509"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/client/gosqldriver/tls"
	"github.com/paypal/hera/utility/logger"
	"os"
	"sync"
)

var occHost = os.Getenv("OCC_TEST_ENV")
var occPort = "10101"
var connMutex sync.Mutex
var idCounter int64

type TestConnection struct {
	conn    *sql.Conn
	txn     *sql.Tx
	context context.Context
	Err     error
	db      *sql.DB
	id      int64
}

func (tc *TestConnection) errorConnection(err error) {
	tc.conn = nil
	tc.db = nil
	tc.context = nil
	tc.txn = nil
	tc.Err = err
	tc.id = -1
}

func (tc *TestConnection) Close() {
	if tc.conn != nil {
		tc.conn.Close()
	}

	if tc.db != nil {
		tc.db.Close()
	}
}

func (tc *TestConnection) SetUpHeraConnection(certPath string) (string, string, error) {
	var tlsEnv = os.Getenv("TLS")
	host := "1:" + occHost + ":" + occPort
	driverName := "heratls"

	if len(tlsEnv) > 0 && tlsEnv == "1" {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "tls enabled")
		}
		tls.HeraTLSDrv.TLSCfg.InsecureSkipVerify = true
		dat, err := os.ReadFile(certPath)
		if err != nil {
			return "", "", err
		}

		rootPEM := string(dat)
		roots := x509.NewCertPool()
		ok := roots.AppendCertsFromPEM([]byte(rootPEM))
		if !ok {
			return "", "", fmt.Errorf("failed to parse root certificate")
		}
		tls.HeraTLSDrv.TLSCfg.RootCAs = roots
		tls.HeraTLSDrv.Ssl = true
		key := []byte{166, 35, 129, 232, 126, 80, 214, 71, 152, 247, 2, 185, 25, 128, 2, 174, 145,
			38, 48, 107, 60, 129, 228, 137, 87, 72, 176, 144, 194, 163, 237, 11}
		tls.HeraTLSDrv.EncryptedAuthKey = key
	} else {
		host = occHost + ":" + occPort
		driverName = "hera"
		tcp.RegisterHeraDriver()
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "tls disabled")
		}
	}
	return host, driverName, nil
}

func (tc *TestConnection) GetConnection() {
	pwd, _ := os.Getwd()
	host, driverName, err := tc.SetUpHeraConnection(pwd + "/../../certs/client_test.cert")
	if err != nil {
		logger.GetLogger().Log(logger.Warning, err)
		tc.errorConnection(err)
		return
	}

	db, err := sql.Open(driverName, host)
	if err != nil {
		logger.GetLogger().Log(logger.Warning, err)
		tc.errorConnection(err)
		return
	}

	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Warning, err)
		tc.errorConnection(err)
		return
	}
	tc.conn = conn
	tc.context = ctx
	tc.db = db
	tc.Err = nil
	connMutex.Lock()
	idCounter += 1
	tc.id = idCounter
	connMutex.Unlock()
}
