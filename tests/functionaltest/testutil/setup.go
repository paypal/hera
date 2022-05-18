package testutil

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"strconv"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
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
	PostgresWorker
)

type DBType int

const (
	Oracle DBType = iota
	MySQL
	PostgreSQL 
)

type mux struct {
	origDir string
	wDir    string
	appcfg  map[string]string
	opscfg  map[string]string
	wType   WorkerType
	wg      sync.WaitGroup
	dbServ  *exec.Cmd
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
	for k,v := range m.opscfg {
		m.appcfg[k] = v
	}

	if m.wType == MySQLWorker {
		m.appcfg["child.executable"] = "mysqlworker"
	} else if m.wType == PostgresWorker {
		m.appcfg["child.executable"] = "postgresworker"
	}
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
	os.Remove("postgresworker")
	if m.wType == OracleWorker {
		os.Symlink(os.Getenv("GOPATH")+"/bin/oracleworker", "oracleworker")
	} else if m.wType == MySQLWorker {
		os.Symlink(os.Getenv("GOPATH")+"/bin/mysqlworker", "mysqlworker")
	} else {
		os.Symlink(os.Getenv("GOPATH")+"/bin/postgresworker", "postgresworker")
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
	os.Remove("postgresworker")
	return nil
}

func UpdateDBState(dbIP string, dbName string, dbType DBType, state int) error {
	if dbIP == "" {
		return errors.New("dbIP is nil")
	}
	preUser := os.Getenv("username")
	prePwd := os.Getenv("password")
	os.Setenv("username", "root")
	os.Setenv("password", "1-testDB")
	var err error
	if dbType == MySQL {	
		q := fmt.Sprintf("SET GLOBAL read_only = %d", state)
		err := DBDirect(q, dbIP, dbName, dbType)
		if err != nil {
			logger.GetLogger().Log(logger.Warning, "update " + dbName + " GLOBAL readonly to " + strconv.Itoa(state) + " failed." + err.Error())
		}
	}
	os.Setenv("username", preUser)
	os.Setenv("password", prePwd)
	return err
}


func setupReadOnly(dbIP string, dbName string, dbType DBType) error {
	if dbIP == "" {
		return errors.New("dbIP is nil")
	}
	if os.Getenv("username") == "" || os.Getenv("password")=="" {
		return errors.New("Failed to set up DB user")
	}

	var err error
	preUser := os.Getenv("username")
	prePwd := os.Getenv("password")
	os.Setenv("username", "root")
	os.Setenv("password", "1-testDB")
	if dbType == MySQL {
		q := "SET GLOBAL read_only = 1;"
		err = DBDirect(q, dbIP, dbName, dbType)
		if err != nil {
			logger.GetLogger().Log(logger.Warning, "set " + dbName + " GLOBAL readonly errored "+err.Error())
		}
	}
	os.Setenv("username", preUser)
	os.Setenv("password", prePwd)
	return err
}

func MakeDB(dockerName string, dbName string, dbType DBType) (ip string) {
	CleanDB(dockerName)
	if dbType == MySQL {
		//Commented out temporarily so we don't have to run docker all the time
		cmd:=exec.Command("docker","run","--name",dockerName,"-e","MYSQL_ROOT_PASSWORD=1-testDb","-e","MYSQL_DATABASE="+dbName,"-d","mysql:latest")
		cmd.Run()

		// find its IP
		cmd=exec.Command("docker","inspect","--format","{{ .NetworkSettings.IPAddress }}",dockerName)
		var ipBuf bytes.Buffer
		cmd.Stdout = &ipBuf
		cmd.Run()
		ipBuf.Truncate(ipBuf.Len()-1)

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

		os.Setenv("username", "root")
		os.Setenv("password", "1-testDb")
		q := "CREATE USER 'appuser'@'%' IDENTIFIED BY '1-testDb'"
		logger.GetLogger().Log(logger.Warning, "set up app user:"+q)
		err := DBDirect(q, ipBuf.String(), dbName, MySQL)
		if err != nil {
			logger.GetLogger().Log(logger.Warning, "set up app user:"+q+" errored "+err.Error())
		}
		q = "GRANT ALL PRIVILEGES ON "+dbName+" . * TO 'appuser'@'%';"
		logger.GetLogger().Log(logger.Warning, "grant  app user:"+q)
		err = DBDirect(q, ipBuf.String(), dbName, MySQL)
		if err != nil {
			logger.GetLogger().Log(logger.Warning, "grant app user:"+q+" errored "+err.Error())
		} else {
			os.Setenv("username", "appuser")
		}

		return ipBuf.String()
	} else if dbType == PostgreSQL {
		cmd := exec.Command("docker", "run", "--name", dockerName, "-e", "POSTGRES_PASSWORD=1-testDb", "-e", "POSTGRES_DB="+dbName, "-d", "postgres:14-alpine")
		cmd.Run()
		// find its IP
		cmd = exec.Command("docker", "inspect", "--format", "{{ .NetworkSettings.IPAddress }}", dockerName)
		var ipBuf bytes.Buffer
		cmd.Stdout = &ipBuf
		cmd.Run()
		ipBuf.Truncate(ipBuf.Len() - 1)
		for {
			conn, err := net.Dial("tcp", ipBuf.String()+":5432")
			if err != nil {
				time.Sleep(1 * time.Second)
				logger.GetLogger().Log(logger.Debug, "waiting for postgres server to come up "+ipBuf.String()+" "+dockerName)
				continue
			} else {
				conn.Close()
				break
			}
		}
		os.Setenv("username", "postgres")
		os.Setenv("password", "1-testDb")
		q := "CREATE USER appuser PASSWORD '1-testDb'"
		logger.GetLogger().Log(logger.Warning, "set up app user:"+q)
		err := DBDirect(q, ipBuf.String(), dbName, PostgreSQL)
		if err != nil {
			logger.GetLogger().Log(logger.Warning, "set up app user:"+q+" errored "+err.Error())
		}
		q = "GRANT ALL PRIVILEGES ON DATABASE " + dbName + " TO appuser;"
		err = DBDirect(q, ipBuf.String(), dbName, PostgreSQL)
		if err != nil {
			logger.GetLogger().Log(logger.Warning, "grant app user:"+q+" errored "+err.Error())
		} else {
			os.Setenv("username", "appuser")
		}
		return ipBuf.String()
	}
	return ""
}
func CleanDB(dockerName string) {
	cleanCmd := exec.Command("docker", "stop", dockerName)
	cleanCmd.Run()
	cleanCmd = exec.Command("docker", "rm", dockerName)
	cleanCmd.Run()
}

var dbs map[string]*sql.DB

func DBDirect(query string, ip string, dbName string, dbType DBType) (error)  {
	if dbs == nil {
		dbs = make(map[string]*sql.DB)
	}
	db0, ok := dbs[ip+dbName]
	if !ok {
		fullDsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s",
			os.Getenv("username"),
			os.Getenv("password"),
			ip,
			dbName)
		//fmt.Println("fullDsn",fullDsn)
		var err error
		db0, err = sql.Open("mysql", fullDsn)
		if err != nil {
			return err
		}
	}
	if dbType == MySQL {
		if !ok {
			fullDsn:=fmt.Sprintf("%s:%s@tcp(%s:3306)/%s",
				os.Getenv("username"),
				os.Getenv("password"),
				ip,
				dbName)
			//fmt.Println("fullDsn",fullDsn)
			var err error
			db0, err = sql.Open("mysql", fullDsn)
			if err != nil {
				return err
			}
			db0.SetMaxIdleConns(0)
			// defer db0.Close()
			dbs[ip+dbName] = db0
		}
	} else if dbType == PostgreSQL {
		if !ok {
			fullDsn := fmt.Sprintf("postgres://%s:%s@%s/%s?connect_timeout=60&sslmode=disable",
				os.Getenv("username"),
				os.Getenv("password"),
				ip,
				dbName)
			var err error
			db0, err = sql.Open("postgres", fullDsn)
			if err != nil {
				return err
			}
			db0.SetMaxIdleConns(0)
			dbs[ip+dbName] = db0
		}
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	conn0, err := db0.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn0.Close()
	stmt0, err := conn0.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt0.Close()
	_, err = stmt0.Exec()
	if err != nil {
		return err
	}
	return nil
}

func (m *mux) StartServer() error {
	// setup working dir
	m.setupWorkdir()
	err := m.setupConfig()
	if err != nil {
		return err
	}
	if m.wType == MySQLWorker {
		xMysql, ok := m.appcfg["x-mysql"]
		if !ok {
			xMysql = "auto"
		}
		if xMysql == "mock" {
			// clean up stray
			cleanCmd := exec.Command("killall", "runserver")
			cleanCmd.Run()

			// spawn test db
			ctx, cancelF := context.WithCancel(context.Background())
			m.dbStop = cancelF
			m.dbServ = exec.CommandContext(ctx, os.Getenv("GOPATH")+"/bin/runserver", "2121", "0.0")
			err := m.dbServ.Start()
			if err != nil {
				logger.GetLogger().Log(logger.Warning, "test mock mysql dbserv did not spawn "+err.Error())
			}

			os.Setenv("username", "herausertest")
			os.Setenv("password", "Hera-User-Test-9")
			os.Setenv("TWO_TASK", "tcp(127.0.0.1:2121)/heratestdb")
			os.Setenv("TWO_TASK_1", "tcp(127.0.0.1:2121)/heratestdb")
			os.Setenv("TWO_TASK_2", "tcp(127.0.0.1:2121)/heratestdb")
			os.Setenv("TWO_TASK_3", "tcp(127.0.0.1:2121)/heratestdb")
			os.Setenv("TWO_TASK_4", "tcp(127.0.0.1:2121)/heratestdb")
		} else if xMysql == "auto" {
			ip := MakeDB("mysql22", "heratestdb", MySQL)
			readpct, err := strconv.Atoi(m.appcfg["readonly_children_pct"])
			if err == nil && readpct > 0 { 
				ipReadonly := MakeDB("mysql23", "heratestdbr", MySQL)
				err = setupReadOnly(ipReadonly, "heratestdbr", MySQL )
				if err == nil {
					os.Setenv("MYSQLR_IP", ipReadonly)
					os.Setenv("TWO_TASK", fmt.Sprintf("tcp(%s:3306)/heratestdb||tcp(%s:3306)/heratestdbr", ip, ipReadonly))
					os.Setenv("TWO_TASK_READ", fmt.Sprintf("tcp(%s:3306)/heratestdbr||tcp(%s:3306)/heratestdb", ipReadonly, ip))
				}
			} else {
				os.Setenv("TWO_TASK", "tcp("+ip+":3306)/heratestdb")
				os.Setenv("TWO_TASK_1", "tcp("+ip+":3306)/heratestdb")
				os.Setenv("TWO_TASK_2", "tcp("+ip+":3306)/heratestdb")
				os.Setenv("TWO_TASK_3", "tcp("+ip+":3306)/heratestdb")
				os.Setenv("TWO_TASK_4", "tcp("+ip+":3306)/heratestdb")
			}
			os.Setenv("MYSQL_IP", ip)
			// Set up the rac_maint table
			pfx := os.Getenv("MGMT_TABLE_PREFIX")
			if pfx == "" {
				pfx = "hera"
			}
			tableName := pfx + "_maint"
			tableString := "create table " + tableName + " ( INST_ID INT,  MACHINE VARCHAR(512),  STATUS VARCHAR(8),  STATUS_TIME INT,  MODULE VARCHAR(64) );"
			DBDirect(tableString, os.Getenv("MYSQL_IP"), "heratestdb", MySQL)
			if readpct > 0 {
				readTable := "create table TABLEINREPLICA (ID INT, STATUS VARCHAR(8), STATUS_TIME INT);"
				DBDirect(tableString, os.Getenv("MYSQLR_IP"), "heratestdbr", MySQL)
				DBDirect(readTable, os.Getenv("MYSQLR_IP"), "heratestdbr", MySQL)
			}
		}
	} else if m.wType == PostgresWorker {
		xPostgres, ok := m.appcfg["x-postgres"]
		if !ok {
			xPostgres = "auto"
		}
		if xPostgres == "auto" {
			ip := MakeDB("postgres22", "heratestdb", PostgreSQL)
			readpct, err := strconv.Atoi(m.appcfg["readonly_children_pct"])
			if err == nil && readpct > 0 { 
				ipReadonly := MakeDB("postgres23", "heratestdbr", PostgreSQL)
				err = setupReadOnly(ipReadonly, "heratestdbr", PostgreSQL)
				if err == nil {
					os.Setenv("PGR_IP", ipReadonly)
					os.Setenv("TWO_TASK", fmt.Sprintf("%s/heratestdb?connect_timeout=60&sslmode=disable||%s/heratestdbr?connect_timeout=60&sslmode=disable", ip, ipReadonly))
					os.Setenv("TWO_TASK_READ", fmt.Sprintf("%s/heratestdbr?connect_timeout=60&sslmode=disable||%s/heratestdb?connect_timeout=60&sslmode=disable", ipReadonly, ip))
				}
			} else {
				os.Setenv("TWO_TASK", ip+"/heratestdb?connect_timeout=60&sslmode=disable")
				twoTask := os.Getenv("TWO_TASK")
				os.Setenv ("TWO_TASK_0", twoTask)
				os.Setenv ("TWO_TASK_1", twoTask)
				twoTask1 := os.Getenv("TWO_TASK")
				fmt.Println ("TWO_TASK_1: ", twoTask1)
				os.Setenv ("TWO_TASK_2", twoTask)
				os.Setenv ("TWO_TASK_3", twoTask)
				os.Setenv ("TWO_TASK_4", twoTask)
			}
			os.Setenv("PG_IP", ip)
			os.Setenv("TWO_TASK_STANDBY0", ip+"/heratestdb?connect_timeout=60&sslmode=disable")
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
		acpt, err := StatelogGetField(2)
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
	if m.wType == MySQLWorker {
		CleanDB("mysql22")
		CleanDB("mysql23")
	} else if m.wType == PostgresWorker {
		CleanDB("postgres22")
		CleanDB("postgres23")
	}

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
