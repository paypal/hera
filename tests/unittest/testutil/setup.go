package testutil

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/paypal/hera/lib"
	"github.com/paypal/hera/utility/logger"
)

type Mux interface {
	StartServer() error
	StopServer()
}

/**
commons used by mux tests
*/

type WorkerType int

const (
	OracleWorker WorkerType = iota
	MySQLWorker
)

type mux struct {
	origDir string
	wDir    string
	appcfg  map[string]string
	opscfg  map[string]string
	wType   WorkerType
	wg      sync.WaitGroup
	dbServ	*exec.Cmd
	dbStop  context.CancelFunc
	// dbIp    string
}

var initialized = false

func NewMux(wType WorkerType, appcfg map[string]string, opscfg map[string]string) (Mux, error) {
	if initialized {
		return nil, errors.New("Mux already created")
	} else {
		initialized = true
	}
	return &mux{appcfg: appcfg, opscfg: opscfg, wType: wType}, nil
}

func createCfg(cfg map[string]string, filename string) error {
	os.Remove(filename + ".txt")
	f, err := os.Create(filename + ".txt")
	if err != nil {
		return err
	}
	for key, val := range cfg {
		f.Write([]byte(key))
		f.Write([]byte("="))
		f.Write([]byte(val))
		f.Write([]byte("\n"))
	}
	f.Close()
	return err
}

func (m *mux) setupWorkdir() {
	m.origDir, _ = os.Getwd()
	path := filepath.Dir(os.Args[0]) + "/"
	os.Chdir(path)
	m.wDir = path
}

func (m *mux) setupConfig() error {
	// opscfg
	m.appcfg["opscfg.hera.server.max_connections"] = m.opscfg["opscfg.default.server.max_connections"]
	m.appcfg["opscfg.hera.server.log_level"] = m.opscfg["opscfg.default.server.log_level"]
	err := createCfg(m.appcfg, "hera")
	if err != nil {
		return err
	}

	// cal_client
	calcfg := make(map[string]string)
	username, err := user.Current()
	if err != nil {
		return err
	}
	calcfg["cal_pool_name"] = username.Username + ".pg_hera"
	calcfg["enable_cal"] = "true"
	calcfg["cal_handler"] = "file"
	calcfg["cal_enable_mlog"] = "false"
	calcfg["cal_log_file"] = "./cal.log"
	calcfg["cal_pool_stack_enable"] = "true"
	err = createCfg(calcfg, "cal_client")
	if err != nil {
		return err
	}

	if m.wType == OracleWorker {
		env := os.Getenv("TWO_TASK")
		if env == "" {
			return errors.New("TWO_TASK env is not defined")
		}
	}
	// mysql (mock or normal) gets username, password, TWO_TASK setup during server start

	os.Remove("oracleworker")
	os.Remove("mysqlworker")
	if m.wType == OracleWorker {
		os.Symlink(os.Getenv("GOPATH")+"/bin/oracleworker", "oracleworker")
	} else {
		os.Symlink(os.Getenv("GOPATH")+"/bin/mysqlworker", "mysqlworker")
	}

	os.Remove("hera.log")
	os.Remove("cal.log")
	os.Remove("state.log")
	_, err = os.Create("state.log")

	return nil
}

func findNextChar(pos int, str string, ch byte) int {
	for {
		if pos < 0 || pos >= len(str) {
			return -1
		}
		if str[pos] == ch {
			return pos
		}
		pos++
	}
}

func (m *mux) cleanupConfig() error {
	os.Remove("hera.txt")
	os.Remove("secret.txt")
	os.Remove("cal_client.txt")
	os.Remove("oracleworker")
	os.Remove("mysqlworker")
	return nil
}

func MakeMysql(dockerName string, dbName string) (ip string) {
	CleanMysql(dockerName)

	cmd:=exec.Command("docker","run","--name",dockerName,"-e","MYSQL_ROOT_PASSWORD=1-testDb","-e","MYSQL_DATABASE="+dbName,"-d","mysql:latest")
	cmd.Run()

	// find its IP
	cmd=exec.Command("docker","inspect","--format","{{ .NetworkSettings.IPAddress }}",dockerName)
	var ipBuf bytes.Buffer
	cmd.Stdout = &ipBuf
	cmd.Run()
	ipBuf.Truncate(ipBuf.Len()-1)

	os.Setenv("username", "root")
	os.Setenv("password", "1-testDb")

        for {
                conn, err := net.Dial("tcp", ipBuf.String()+":3306")
                if err != nil {
                        time.Sleep(1 * time.Second)
                        logger.GetLogger().Log(logger.Debug, "waiting for mysql server to come up "+ipBuf.String()+" "+dockerName)
                        continue
                } else {
                        conn.Close()
                        break
                }
        }

	return ipBuf.String()
}
func CleanMysql(dockerName string) {
	cleanCmd := exec.Command("docker", "stop", dockerName)
	cleanCmd.Run()
	cleanCmd = exec.Command("docker", "rm", dockerName)
	cleanCmd.Run()
}

func (m *mux) StartServer() error {
	// setup working dir
	m.setupWorkdir()
	err := m.setupConfig()
	if err != nil {
		return err
	}
	if m.wType != OracleWorker {
		xMysql, ok := m.appcfg["x-mysql"]
		if !ok {
			xMysql = "auto"
		}
		if xMysql == "mock" {
			// clean up stray
			cleanCmd := exec.Command("killall", "runserver")
			cleanCmd.Run()

			// spawn test db
			ctx,cancelF := context.WithCancel(context.Background())
			m.dbStop = cancelF
			m.dbServ = exec.CommandContext(ctx, os.Getenv("GOPATH")+"/bin/runserver", "2121", "0.0")
			err := m.dbServ.Start()
			if err != nil {
				logger.GetLogger().Log(logger.Warning, "test mock mysql dbserv did not spawn " + err.Error())
			}

			os.Setenv("username", "herausertest")
			os.Setenv("password", "Hera-User-Test-9")
			os.Setenv("TWO_TASK", "tcp(127.0.0.1:2121)/heratestdb")
		} else if xMysql == "auto" {
			ip := MakeMysql("mysql22", "heratestdb")
			os.Setenv("TWO_TASK", "tcp("+ip+":3306)/heratestdb")
		}
	}

	m.wg.Add(1)
	go func() {
		// run the multiplexer
		os.Args = append(os.Args, "--name", "hera-test")
		lib.Run()
		m.wg.Done()
	}()

	// wait 10 seconds for mux to come up
	toWait := 10
	for {
		acpt, err := statelogGetField(2)
		if err == nil || err == INCOMPLETE {
			logger.GetLogger().Log(logger.Debug, "State log acpt:", acpt)
			if err == nil {
				if acpt > 0 {
					break
				}
			}
			if toWait == 0 {
				logger.GetLogger().Log(logger.Alert, "Mux did not start")
				return errors.New("")
			}
			logger.GetLogger().Log(logger.Debug, "Wait up to ", toWait, "seconds for mux to come up")
			time.Sleep(time.Second)
			toWait--
		} else {
			return err
		}
	}
	logger.GetLogger().Log(logger.Info, "StartServer: Mux is up, time =", time.Now().Unix())
	return nil
}

func (m *mux) StopServer() {
	syscall.Kill(os.Getpid(), syscall.SIGTERM)
	if m.dbServ != nil {
		m.dbStop()
		syscall.Kill((*m.dbServ).Process.Pid, syscall.SIGTERM)
	}
	CleanMysql("mysql22")

	timer := time.NewTimer(time.Second * 5)
	go func() {
		logger.GetLogger().Log(logger.Debug, "Waiting up to 5 seconds for mux to exit", time.Now().Unix())
		_, ok := <-timer.C
		if ok {
			logger.GetLogger().Log(logger.Alert, "Mux did not shut down", time.Now().Unix())
		}
		m.wg.Done()
	}()

	m.wg.Wait()
	timer.Stop()
	m.cleanupConfig()
	os.Chdir(m.origDir)
	logger.GetLogger().Log(logger.Info, "Exit StopServer time=", time.Now().Unix())
}
