package util

import "C"
import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/paypal/hera/utility/logger"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

var CT = ClientTraffic{ReadTraffic: true, WriteTraffic: true, TransactionTraffic: true, InProgress: false}

type queryStats struct {
	successCount int
	failureCount int
}

type ClientTraffic struct {
	ReadTraffic        bool
	WriteTraffic       bool
	TransactionTraffic bool
	InProgress         bool
}

type ClientTrafficStats struct {
	// string - type of query ReadType, WriteType, TXNType
	// int - utc in seconds
	// queryStats - success and failure
	stats map[string]map[int]*queryStats
}

func (ct ClientTraffic) getNextValFromTxn(txn *sql.Tx, ctx context.Context) (int, error) {
	query := "select id_seq.NEXTVAL FROM dual"
	stmt, _ := txn.PrepareContext(ctx, query)
	rows, err := stmt.Query()
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	rows.Next()
	var id int
	if err := rows.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (ct ClientTraffic) getNextVal(conn *sql.Conn, ctx context.Context) (int, error) {
	query := "select id_seq.NEXTVAL FROM dual"
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	return 0, errors.New("should not have reached")
}

func (ct ClientTraffic) WriteInTxn(c TestConnection) (int, error) {

	txn, err := c.conn.BeginTx(c.context, nil)
	if err != nil {
		txn.Rollback()
		return 0, err
	}
	c.txn = txn

	insertQuery := "insert into occ_test values(id_seq.NEXTVAL, ?, ?)"
	stmt, _ := c.txn.PrepareContext(c.context, insertQuery)
	_, err = stmt.Exec("txn-record", 1)
	if err != nil {
		txn.Rollback()
		return 0, err
	}

	dbId, err := ct.identifyDBTxn(txn, c.context)
	if err != nil {
		txn.Rollback()
		return 0, err
	}

	id, Err := ct.getNextValFromTxn(c.txn, c.context)
	if Err != nil {
		txn.Rollback()
		return 0, Err
	}
	insertQuery = fmt.Sprintf("insert into occ_test values(%d, 'txnExample', 1)", id)

	stmt, _ = c.txn.PrepareContext(c.context, insertQuery)
	_, Err = stmt.Exec()
	if Err != nil {
		txn.Rollback()
		return 0, Err
	}

	updateQuery := fmt.Sprintf("update occ_test set version = 2 where id =%d", id)
	stmt, _ = c.txn.PrepareContext(c.context, updateQuery)
	_, Err = stmt.Exec()
	if Err != nil {
		txn.Rollback()
		return 0, Err
	}

	err = txn.Commit()
	if err != nil {
		txn.Rollback()
		return 0, err
	}

	return dbId, nil

}

func (ct ClientTraffic) identifyDBTxn(txn *sql.Tx, ctx context.Context) (int, error) {
	query := "select id FROM db_id_test"
	stmt, _ := txn.PrepareContext(ctx, query)
	rows, err := stmt.Query()
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	rows.Next()
	var id int
	if err := rows.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (ct ClientTraffic) slowIdentifyDB(conn *sql.Conn, ctx context.Context, sec int) (int, error) {
	query := fmt.Sprintf("select SLOW_QUERY(%d) from dual", sec)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	return 0, errors.New("should not have reached")
}

func (ct ClientTraffic) identifyDB(conn *sql.Conn, ctx context.Context) (int, error) {
	query := "select id FROM db_id_test"
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	return 0, errors.New("should not have reached")
}

func initQueryStats() map[int]*queryStats {
	r := make(map[int]*queryStats)

	r[0] = &queryStats{0, 0}
	r[1] = &queryStats{0, 0}
	r[2] = &queryStats{0, 0}
	return r
}

func (ct ClientTraffic) checkAndCreateStruct(utc int64, CTS map[int64]ClientTrafficStats) {
	timeMutex.Lock()
	_, ok := CTS[utc]

	if !ok {
		v := make(map[string]map[int]*queryStats)

		v[READ] = initQueryStats()
		v[WRITE] = initQueryStats()
		v[TXN] = initQueryStats()

		CTS[utc] = ClientTrafficStats{v}
	}
	timeMutex.Unlock()

}

func (ct ClientTraffic) CreateCounter(utc int64, counterType string, CTS map[int64]ClientTrafficStats) {
	ct.checkAndCreateStruct(utc, CTS)
	m := ct.getMutexForType(counterType)
	statMutex.Lock()
	m.Lock()
	_, ok := CTS[utc].stats[counterType]
	if !ok {
		CTS[utc].stats[counterType][0] = &queryStats{}
		CTS[utc].stats[counterType][1] = &queryStats{}
		CTS[utc].stats[counterType][2] = &queryStats{}
	}
	m.Unlock()
	statMutex.Unlock()
}

func (ct ClientTraffic) ReadQuery(query string) (int, error) {
	c := TestConnection{}
	c.GetConnection()
	rows, err := c.conn.QueryContext(c.context, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	return 0, errors.New("should not have reached")
}

func (ct ClientTraffic) txnTraffic(CTS map[int64]ClientTrafficStats, n int64) {
	ct.CreateCounter(n, TXN, CTS)
	dbId := 0
	var err error

	c := TestConnection{}
	c.GetConnection()

	if c.Err != nil {
		ct.incrementFailure(TXN, dbId, CTS[n].stats, c.Err)
		return
	}

	defer c.Close()
	dbId, err = ct.WriteInTxn(c)
	if err != nil {
		ct.incrementFailure(TXN, dbId, CTS[n].stats, err)
	} else {
		ct.incrementSuccess(TXN, dbId, CTS[n].stats)
	}
}

func (ct ClientTraffic) writeTraffic(CTS map[int64]ClientTrafficStats, n int64) {
	ct.CreateCounter(n, WRITE, CTS)
	id := 0
	c := TestConnection{}
	c.GetConnection()
	failed := true

	if c.Err != nil {
		logger.GetLogger().Log(logger.Alert, "Write ", c.Err)
		ct.incrementFailure(WRITE, id, CTS[n].stats, c.Err)
		return
	}

	defer c.Close()
	insertQuery := "insert into occ_test values(id_seq.NEXTVAL, 'write-record', 1)"

	txn, err := c.conn.BeginTx(c.context, nil)

	if err != nil {
		ct.incrementFailure(WRITE, id, CTS[n].stats, err)
		txn.Rollback()
		return
	}
	_, err = txn.ExecContext(c.context, insertQuery)

	if err == nil {
		id, err = ct.identifyDBTxn(txn, c.context)
		if err == nil {
			err = txn.Commit()
			failed = false
		} else {
			logger.GetLogger().Log(logger.Alert, "Write ", err)
			txn.Rollback()
		}
	} else {
		logger.GetLogger().Log(logger.Alert, "Write ", err)
		txn.Rollback()
	}

	if failed {
		ct.incrementFailure(WRITE, id, CTS[n].stats, err)
	} else {
		ct.incrementSuccess(WRITE, id, CTS[n].stats)
	}

}

func (ct ClientTraffic) slowReadTraffic(CTS map[int64]ClientTrafficStats, n int64, sec int) {
	ct.CreateCounter(n, READ, CTS)
	id := 0
	var err error
	c := TestConnection{}
	c.GetConnection()
	if c.Err != nil {
		if c.Err.Error() == "Failed to read server info" {
			logger.GetLogger().Log(logger.Alert, "Enabling TLS on Client Side as server side it is enabled")
			os.Setenv("TLS", "1")
			return
		} else {
			ct.incrementFailure(READ, id, CTS[n].stats, c.Err)
			return
		}
	}

	defer c.Close()
	id, err = ct.slowIdentifyDB(c.conn, c.context, sec)

	if err != nil {
		logger.GetLogger().Log(logger.Alert, err)
		ct.incrementFailure(READ, id, CTS[n].stats, err)
	} else {
		ct.incrementSuccess(READ, id, CTS[n].stats)
	}
}

func (ct ClientTraffic) readTraffic(CTS map[int64]ClientTrafficStats, n int64) {
	ct.CreateCounter(n, READ, CTS)
	id := 0
	var err error
	c := TestConnection{}
	c.GetConnection()
	if c.Err != nil {
		if c.Err.Error() == "Failed to read server info" {
			logger.GetLogger().Log(logger.Alert, "Enabling TLS on Client Side as server side it is enabled")
			os.Setenv("TLS", "1")
			return
		} else {
			logger.GetLogger().Log(logger.Alert, "failed in connect "+c.Err.Error())
			ct.incrementFailure(READ, id, CTS[n].stats, c.Err)
			return
		}
	}

	defer c.Close()
	id, err = ct.identifyDB(c.conn, c.context)

	if err != nil {
		ct.incrementFailure(READ, id, CTS[n].stats, err)
	} else {
		ct.incrementSuccess(READ, id, CTS[n].stats)
	}
}

func (ct ClientTraffic) DumpStats(CTS map[int64]ClientTrafficStats) {
	logger.GetLogger().Log(logger.Alert, "**************")
	logger.GetLogger().Log(logger.Alert, "QueryStats")
	logger.GetLogger().Log(logger.Alert, "**************")
	logger.GetLogger().Log(logger.Alert, "UTC:\t\tDB1 Read Success/Fail\t\tDB2 Read Success/Fail\tDB1 Write Success/Fail"+
		"\tDB2 Write Success/Fail\tDB1 Txn Success/Fail\tDB2 Tx Success/Fail\nAll DB Failure")
	keys := make([]int64, 0)
	for k, _ := range CTS {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, utc := range keys {
		cts := CTS[utc]
		logger.GetLogger().Log(logger.Alert,
			utc, "\t\t\t",
			cts.stats[READ][1].successCount, "/", cts.stats[READ][1].failureCount, "\t\t\t",
			cts.stats[READ][2].successCount, "/", cts.stats[READ][2].failureCount, "\t\t\t",
			cts.stats[WRITE][1].successCount, "/", cts.stats[WRITE][1].failureCount, "\t\t\t",
			cts.stats[WRITE][2].successCount, "/", cts.stats[WRITE][2].failureCount, "\t\t\t",
			cts.stats[TXN][1].successCount, "/", cts.stats[TXN][1].failureCount, "\t\t\t",
			cts.stats[TXN][2].successCount, "/", cts.stats[TXN][2].failureCount, "\t\t\t",
			cts.stats[READ][0].failureCount+cts.stats[WRITE][0].failureCount+cts.stats[TXN][0].failureCount)
	}

}

func (ct ClientTraffic) SendClientTraffic(wg *sync.WaitGroup) (chan map[int64]ClientTrafficStats, chan map[int64]ClientTrafficStats, chan string) {
	wg.Add(1)
	CTSChan := make(chan map[int64]ClientTrafficStats)
	DumpChan := make(chan map[int64]ClientTrafficStats)
	RunMsg := make(chan string)
	go ct.traffic(wg, RunMsg, CTSChan, DumpChan)
	return CTSChan, DumpChan, RunMsg
}

func (ct ClientTraffic) StopClientTraffic(CTSChan chan map[int64]ClientTrafficStats, RunMsg chan string) map[int64]ClientTrafficStats {
	RunMsg <- STOP
	d := <-CTSChan
	//ct.DumpStats(d)
	return d
}

func (ct ClientTraffic) DumpTrafficStat(DumpLogChan chan map[int64]ClientTrafficStats, RunMsg chan string) map[int64]ClientTrafficStats {
	logger.GetLogger().Log(logger.Alert, "DumpTrafficStat")
	RunMsg <- DumpLogs
	d := <-DumpLogChan
	return d
}

func (ct ClientTraffic) TearDown(respChan chan map[int64]ClientTrafficStats, dumpChan chan map[int64]ClientTrafficStats,
	msgChan chan string, file *os.File) {
	logger.GetLogger().Log(logger.Alert, "Traffic InProgress ", ct.InProgress)
	if ct.InProgress {
		msgChan <- KILL
	}
	close(respChan)
	close(dumpChan)
	close(msgChan)
	_ = (*os.File).Sync(file)
	_ = file.Close()
}
func (ct ClientTraffic) LongReadTraffic(wg *sync.WaitGroup,
	CTChan chan map[int64]ClientTrafficStats, numOfTxn int, delay int, t *testing.T) {
	defer wg.Done()

	CTS := make(map[int64]ClientTrafficStats)

	logger.GetLogger().Log(logger.Alert, "Sending LongReadTraffic")
	n := time.Now().Unix()
	counter := 0
	var wg1 sync.WaitGroup
	for {
		counter += 1
		logger.GetLogger().Log(logger.Alert, "Request ", counter)
		if counter >= numOfTxn {
			break
		}
		wg1.Add(1)
		go func() {
			defer wg1.Done()
			ct.slowReadTraffic(CTS, n, delay)
		}()
	}
	logger.GetLogger().Log(logger.Alert, "waiting to finish the job")
	wg1.Wait()
	logger.GetLogger().Log(logger.Alert, "Sending Stats back")
	CTChan <- CTS
}

func (ct ClientTraffic) LongTxnTraffic(wg *sync.WaitGroup,
	CTChan chan map[int64]ClientTrafficStats, RunMsg chan string, numOfTxn int, delay int, t *testing.T) {
	defer wg.Done()

	CTS := make(map[int64]ClientTrafficStats)
	var dbWriteTrans []*DBTxn

	logger.GetLogger().Log(logger.Alert, "Sending Txn Traffic")
	n := time.Now().Unix()
	ct.CreateCounter(n, TXN, CTS)
	ct.CreateCounter(n, WRITE, CTS)
	ct.CreateCounter(n, READ, CTS)
	counter := 0
	for {
		logger.GetLogger().Log(logger.Alert, "counter ", counter)
		if counter >= numOfTxn {
			break
		}
		counter += 1
		txn, err := writeBeginTxn()
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "writeBeginTxn failure ", err)
			t.Fatalf("txn suppose not to fail here")
		}

		_, err = ct.identifyDBTxn(txn.DBTransaction, txn.DBConnection.context)
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "identifyDBTxn failure ", err)
			t.Fatalf("txn suppose not to fail here")
		}
		dbWriteTrans = append(dbWriteTrans, txn)
	}

	logger.GetLogger().Log(logger.Alert, "locked ", delay)
	RunMsg <- "Locked"
	time.Sleep(time.Duration(delay) * time.Second)
	logger.GetLogger().Log(logger.Alert, "Running Slow Query with delay ", delay)
	var wg1 sync.WaitGroup
	for _, txn := range dbWriteTrans {

		wg1.Add(1)
		txn := txn
		go func() {
			defer wg1.Done()
			logger.GetLogger().Log(logger.Alert, "slow query")
			dbId, err := ct.slowIdentifyDB(txn.DBConnection.conn, txn.DBConnection.context, delay)
			if err != nil {
				logger.GetLogger().Log(logger.Alert, "Txn failure ", err)
				ct.incrementFailure(TXN, dbId, CTS[n].stats, err)
			} else {
				ct.incrementSuccess(TXN, dbId, CTS[n].stats)
			}
		}()
	}

	logger.GetLogger().Log(logger.Alert, "waiting to finish the job")
	wg1.Wait()

	for _, dbTxn := range dbWriteTrans {
		rollbackTxn(dbTxn)
	}
	CTChan <- CTS
}

func (ct ClientTraffic) deepCopyCTS(original ClientTrafficStats) ClientTrafficStats {
	copy := ClientTrafficStats{stats: make(map[string]map[int]*queryStats)}
	for key, value := range original.stats {
		copy.stats[key] = make(map[int]*queryStats)
		for k1, v1 := range value {
			n := queryStats{successCount: v1.successCount, failureCount: v1.failureCount}
			copy.stats[key][k1] = &n
		}
	}

	return copy
}

func (ct ClientTraffic) deepCopyCTSMap(original map[int64]ClientTrafficStats) map[int64]ClientTrafficStats {
	copy := make(map[int64]ClientTrafficStats)

	timeMutex.Lock()
	for key, value := range original {
		copy[key] = ct.deepCopyCTS(value)
	}
	timeMutex.Unlock()
	return copy
}

func (ct ClientTraffic) traffic(wg *sync.WaitGroup, runMsg chan string,
	CTChan chan map[int64]ClientTrafficStats, DumpChan chan map[int64]ClientTrafficStats) {
	defer wg.Done()

	CTS := make(map[int64]ClientTrafficStats)

	started := false
	logger.GetLogger().Log(logger.Alert, "Sending Client Traffic")
	for {
		select {
		case inp := <-runMsg:
			switch inp {
			case KILL:
				logger.GetLogger().Log(logger.Alert, "Kill - Client Traffic")
				ct.InProgress = false
				return
			case STOP:
				logger.GetLogger().Log(logger.Alert, "Stopping Client Traffic")
				logger.GetLogger().Log(logger.Alert, "Traffic InProgress ", ct.InProgress)
				ct.InProgress = false
				CTChan <- CTS
				return
			case DumpLogs:
				logger.GetLogger().Log(logger.Alert, "dumping logs")
				DumpChan <- ct.deepCopyCTSMap(CTS)
			}
		default:
			n := time.Now().Unix()
			if !started {
				logger.GetLogger().Log(logger.Alert, "Traffic StartTime ", n)
				ct.InProgress = true
				logger.GetLogger().Log(logger.Alert, "Traffic InProgress ", ct.InProgress)
			}
			go ct.readTraffic(CTS, n)
			go ct.txnTraffic(CTS, n)
			go ct.writeTraffic(CTS, n)
			started = true
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (ct ClientTraffic) getMutexForType(qsType string) *sync.Mutex {
	if qsType == READ {
		return &readMutex
	}
	if qsType == WRITE {
		return &writeMutex
	}
	if qsType == TXN {
		return &txnMutex
	}
	panic("unknown type")
}

func (ct ClientTraffic) incrementSuccess(qsType string, dbId int, stats map[string]map[int]*queryStats) {
	m := ct.getMutexForType(qsType)
	m.Lock()
	stats[qsType][dbId].successCount += 1
	m.Unlock()
}

func (ct ClientTraffic) incrementFailure(qsType string, dbId int, stats map[string]map[int]*queryStats, err error) {
	m := ct.getMutexForType(qsType)
	m.Lock()
	stats[qsType][dbId].failureCount += 1
	m.Unlock()
}
