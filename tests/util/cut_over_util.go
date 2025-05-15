package util

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/paypal/hera/utility/logger"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var timeMutex sync.Mutex
var statMutex sync.Mutex
var readMutex sync.Mutex
var writeMutex sync.Mutex
var txnMutex sync.Mutex
var lockMutex sync.Mutex

var maxRetryCount = 10
var binaryPushDone = make(map[string]bool)
var READ = "ReadType"
var WRITE = "WriteType"
var TXN = "TXNType"
var UPDATELOCK = "UpdateLock"
var STOP = "stop"
var KILL = "kill"
var DumpLogs = "dumpLogs"
var CreateTable = "CUT_OVER_CREATE"
var CleanCutOver = "CUT_OVER_CLEAN"
var CutOverEnable = "CUT_OVER_ENABLE"
var CutOverEnableWrongOCC = "CUT_OVER_ENABLE_WRONG_OCC"
var CutOverEnableInvalidUniqueName = "CUT_OVER_ENABLE_INVALID_UNIQUE_NAME"
var CutOverEnableInvalidTNS = "CUT_OVER_ENABLE_INVALID_TNS"
var CutOverEnableInvalidPhase = "CUT_OVER_ENABLE_INVALID_PHASE"
var CutOverEnableInvalidRead = "CUT_OVER_ENABLE_INVALID_READ"
var CutOverEnableInvalidWrite = "CUT_OVER_ENABLE_INVALID_WRITE"
var CutOverEnableDualWrite = "CUT_OVER_ENABLE_DUAL_WRITE"
var CutOverEnableDualRead = "CUT_OVER_ENABLE_DUAL_READ"
var CutOverEnableWriteNoRead = "CUT_OVER_ENABLE_WRITE_NO_READ"
var FlexUp = "FLEXUP"
var CutOverFlexUpInValidUniqName = "CUT_OVER_FLEXUP_INVALID_UNIQ_NAME"
var CutOverPhaseI = "CUT_OVER_PHASE_1"
var CutOverPhaseAlone = "CUT_OVER_ALONE"
var CutOverFlexUpInCorrectRole = "CUT_OVER_FLEXUP_INCORRECT_ROLE"
var CutOverPhaseIInvalidRowCount = "CUT_OVER_PHASE_1_INVALID_ROW_CNT"
var CutOverPhaseIIInvalidRowCount = "CUT_OVER_PHASE_2_INVALID_ROW_CNT"
var CutOverPhaseIIIInvalidRowCount = "CUT_OVER_PHASE_3_INVALID_ROW_CNT"
var CutOverCompletePhaseInvalidRowCount = "CUT_OVER_COMPLETE_PHASE_INVALID_ROW_CNT"
var CutOverPhaseIIInvalidDBUniqName = "CUT_OVER_PHASE_2_INVALID_UNIQ_NAME"
var CutOverPhaseIIIInvalidDBUniqName = "CUT_OVER_PHASE_3_INVALID_UNIQ_NAME"
var CutOverCompletePhaseInvalidDBUniqName = "CUT_OVER_COMPLETE_PHASE_INVALID_UNIQ_NAME"
var CutOverPhaseIIInvalidOCCName = "CUT_OVER_PHASE_2_INVALID_OCC_NAME"
var CutOverPhaseIIIInvalidOCCName = "CUT_OVER_PHASE_3_INVALID_OCC_NAME"
var CutOverCompletePhaseInvalidOCCName = "CUT_OVER_COMPLETE_PHASE_INVALID_OCC_NAME"
var CutOverPhaseIIInvalidTwoTask = "CUT_OVER_PHASE_2_INVALID_TWO_TASK"
var CutOverPhaseIIIInvalidTwoTask = "CUT_OVER_PHASE_3_INVALID_TWO_TASK"
var CutOverCompletePhaseInvalidTwoTask = "CUT_OVER_COMPLETE_PHASE_INVALID_TWO_TASK"
var CutOverPhaseIIInvalidPhase = "CUT_OVER_PHASE_2_INVALID_PHASE"
var CutOverPhaseIIIInvalidPhase = "CUT_OVER_PHASE_3_INVALID_PHASE"
var CutOverCompletePhaseInvalidPhase = "CUT_OVER_COMPLETE_PHASE_INVALID_PHASE"
var CutOverPhaseIIIInvalidRead = "CUT_OVER_PHASE_3_INVALID_READ"
var CutOverCompletePhaseInvalidRead = "CUT_OVER_COMPLETE_PHASE_INVALID_READ"
var CutOverPhaseIIIInvalidWrite = "CUT_OVER_PHASE_3_INVALID_WRITE"
var CutOverCompletePhaseInvalidWrite = "CUT_OVER_COMPLETE_PHASE_INVALID_WRITE"
var CutOverPhaseIIIDualWrite = "CUT_OVER_PHASE_3_DUAL_WRITE"
var CutOverCompletePhaseDualWrite = "CUT_OVER_COMPLETE_PHASE_DUAL_WRITE"
var CutOverPhaseIIIDualRead = "CUT_OVER_PHASE_3_DUAL_READ"
var CutOverCompletePhaseDualRead = "CUT_OVER_COMPLETE_PHASE_DUAL_READ"
var CutOverPhaseIIIReadOff = "CUT_OVER_PHASE_3_READ_OFF"
var CutOverCompletePhaseReadOff = "CUT_OVER_COMPLETE_PHASE_READ_OFF"
var CutOverPhaseIInvalidUniqName = "CUT_OVER_PHASE_1_INVALID_UNIQ_NAME"
var CutOverPhaseICorrectUniqName = "CUT_OVER_PHASE_1_CORRECT_UNIQ_NAME"
var CutOverPhaseIInvalidOCCName = "CUT_OVER_PHASE_1_INVALID_OCC_NAME"
var CutOverPhaseIInvalidPhase = "CUT_OVER_PHASE_1_INVALID_PHASE"
var CutOverPhaseIInvalidWriteStatus = "CUT_OVER_PHASE_1_INVALID_WRITE"
var CutOverPhaseIInvalidReadStatus = "CUT_OVER_PHASE_1_INVALID_READ"
var CutOverPhaseIDualWrite = "CUT_OVER_PHASE_1_DUAL_WRITE"
var CutOverPhaseIDualRead = "CUT_OVER_PHASE_1_DUAL_READ"
var CutOverPhaseIReadOff = "CUT_OVER_PHASE_1_READ_OFF"
var CutOverPhaseIInvalidTwoTask = "CUT_OVER_PHASE_1_INVALID_TWO_TASK"
var CutOverPhaseII = "CUT_OVER_PHASE_2"
var CutOverPhaseIII = "CUT_OVER_PHASE_3"
var SourceTargetFlip = "SRC_TO_TGT_FLIP"
var FlipEnable = "FLIP_ENABLE"
var CutOverPhaseIIIWithoutRole = "CUT_OVER_PHASE_3_WITHOUT_ROLE"
var DeleteCutOverTable = "CUT_OVER_TABLE_DELETE"
var CutOverEnableInvalidNumRows = "CUT_OVER_INVALID_NUM_ROWS"

var heraBoxHost = os.Getenv("OCC_TEST_ENV")

type DatabaseServices struct {
	ServiceName string
	WorkerCount int
	Active      bool
}

type DBStatus struct {
	Database         string
	DBUniqueName     string
	ClocAppStatus    string
	DatabaseServices []DatabaseServices
}

type DBTxn struct {
	DBConnection  TestConnection
	DBTransaction *sql.Tx
}

func dbService(dbUniqueName string, serviceName string, action string) []DBStatus {
	var status []DBStatus

	response := httpGet(nil, "http://"+heraBoxHost+":8000/occ/db_service?action="+action+
		"&service_name="+serviceName+"&db_unique_name="+dbUniqueName)
	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(responseData, &status)
	if err != nil {
		panic(err)
	}
	return status
}

func splitBySpace(input string) []string {
	words := strings.Split(input, " ")
	var fields []string

	for _, word := range words {
		if word != "" {
			fields = append(fields, word)
		}
	}
	return fields
}

func TearDown(t *testing.T, respChan chan map[int64]ClientTrafficStats, dumpChan chan map[int64]ClientTrafficStats,
	msgChan chan string, file *os.File) {
	logger.GetLogger().Log(logger.Alert, "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	logger.GetLogger().Log(logger.Alert, "ENDING TEST "+t.Name())
	logger.GetLogger().Log(logger.Alert, "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	CT.TearDown(respChan, dumpChan, msgChan, file)
}

func Setup(t *testing.T) ([]DBStatus, *os.File) {
	path := os.Getenv("TEST_OUTPUT_PATH")
	if path == "" {
		path = "./"
	}
	_, file := logger.CreateLoggerInternal(path+"/"+t.Name()+".log", "UT", logger.Alert, false)
	logger.GetLogger().Log(logger.Alert, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	logger.GetLogger().Log(logger.Alert, "STARTING TEST "+t.Name())
	logger.GetLogger().Log(logger.Alert, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	status := InitialSetup(t)
	return status, file
}

func InitialSetup(t *testing.T) []DBStatus {

	logger.GetLogger().Log(logger.Alert, "********************************")
	logger.GetLogger().Log(logger.Alert, "SETTING THE ENV TO INITIAL SETUP")
	logger.GetLogger().Log(logger.Alert, "********************************")
	if heraBoxHost == "" {
		logger.GetLogger().Log(logger.Alert, "Please set OCC_TEST_ENV env variable to the ip where herabox is deployed")
		t.Fatalf("Please set OCC_TEST_ENV env variable to the ip where herabox is deployed")
	}
	logger.GetLogger().Log(logger.Alert, "USING "+heraBoxHost)

	OCCBinarySetup(t, os.Getenv("GOPATH")+"/src/bin/mux", "mux")
	OCCBinarySetup(t, os.Getenv("GOPATH")+"/src/bin/oracleworker19c", "oracleworker19c")
	ResetOCCDocker(t)

	// disable read write split feature
	OCCConfig(t, "readonly_children_pct", "0", "/x/web/LIVE/occ/occ.cdb")
	OCCConfig(t, "enable_cutover", "false", "/x/web/LIVE/occ/occ.cdb")
	//EnableDebugLog(t)

	// delete all the entries in the cut over metadata table
	MoveCutOverPhase(t, DeleteCutOverTable, true, true)
	GiveRWToPrimary(t)
	GiveROToSecondary(t)
	OCCConfig(t, "cal_handler", "file", "/x/web/LIVE/occ/cal_client.cdb")
	OCCConfig(t, "cal_log_file", "./logs/logCalClient.txt", "/x/web/LIVE/occ/cal_client.cdb")
	// restart occ (without restarting docker) to pick the changes
	RestartOCC(t, 0)

	// prepare db to initial state
	StartDBService("HERADB_ONE", "herabox_primary_srv", t)
	StartDBService("HERADB_TWO", "herabox_secondary_srv", t)

	ShutDownDBService("HERADB_ONE", "herabox_secondary_srv", t)
	ShutDownDBService("HERADB_TWO", "herabox_primary_srv", t)

	LockUnlockUser(t, "unlock", false)

	// validate if we are good in initial state
	dbStatus, _ := GetDBStatus()
	for _, db := range dbStatus {
		for _, service := range db.DatabaseServices {
			if strings.TrimSpace(db.DBUniqueName) == "HERADB_TWO" &&
				strings.TrimSpace(service.ServiceName) == "herabox_primary_srv" && service.WorkerCount > 0 {
				KillSessions(t, true, "herabox_primary_srv")
				logger.GetLogger().Log(logger.Alert, "Sleeping for 120 seconds for connection to jump back to main db")
				time.Sleep(120 * time.Second)
			}
		}
	}
	dbStatus = LockUnlockUser(t, "unlock", true)

	ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)

	logger.GetLogger().Log(logger.Alert, "********************************")
	logger.GetLogger().Log(logger.Alert, "END OF INITIAL SETUP")
	logger.GetLogger().Log(logger.Alert, "********************************")
	return dbStatus
}

func ValidateStateLog(t *testing.T, expected map[string]int, fail bool) bool {
	logger.GetLogger().Log(logger.Alert, "Validating State Logs")
	retryCount := 0
	for {
		url := "http://" + heraBoxHost + ":8000/occ/logs?path=/x/web/LIVE/occ/state-logs/current"
		response := httpGet(t, url)
		byteArray, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf(err.Error())
		}
		str := string(byteArray)
		buf := bytes.NewBufferString(str)
		scanner := bufio.NewScanner(buf)
		for scanner.Scan() {
			line := scanner.Text()
			words := splitBySpace(line)
			workerName := words[2]
			suffix := ".not_connected"
			notConnected := false
			expectedWorkerCount, exists := expected[workerName]
			if !exists {
				expectedWorkerCount, exists = expected[workerName+suffix]
				if exists {
					notConnected = true
				}
			}
			if exists {
				accept, _ := strconv.Atoi(words[4])
				wait, _ := strconv.Atoi(words[5])
				busy, _ := strconv.Atoi(words[6])
				init, _ := strconv.Atoi(words[3])
				scheduled, _ := strconv.Atoi(words[7])
				connected := accept + wait + busy
				connectFailed := init + scheduled

				actualWorkerCount := connected
				msg := "(accept+wait+busy)"
				if notConnected {
					actualWorkerCount = connectFailed
					msg = "(init+schd)"
				}

				if expectedWorkerCount != actualWorkerCount && retryCount > maxRetryCount {
					if fail {
						t.Fatalf("State Log Validation failed for %s at %s %s - "+
							"Expected Worker Count: %d vs Actual %d (%s)",
							workerName, words[0], words[1], expectedWorkerCount, actualWorkerCount, msg)
					} else {
						return false
					}
				} else if expectedWorkerCount == actualWorkerCount {
					if notConnected {
						delete(expected, workerName+suffix)
					} else {
						delete(expected, workerName)
					}
				} else if expectedWorkerCount != actualWorkerCount && retryCount < maxRetryCount {
					logger.GetLogger().Log(logger.Alert, "State Log Validation failed for ", workerName,
						" at ", words[0], " ", words[1],
						" - Expected Worker Count: ", expectedWorkerCount, " vs Actual ", actualWorkerCount, msg,
						" - retrying after 5 seconds")
					break
				}
			}
		}
		if retryCount > maxRetryCount || len(expected) == 0 {
			break
		} else {
			logger.GetLogger().Log(logger.Alert, "Still need to validate:")
			logger.GetLogger().Log(logger.Alert, expected)
		}
		retryCount += 1
		logger.GetLogger().Log(logger.Alert, "Retry - Validating State Logs after sleeping for 8 sec")
		time.Sleep(8 * time.Second)
	}
	for key := range expected {
		if fail {
			t.Fatalf("Unable to find state log for %s", key)
		} else {
			return false
		}
	}
	return true
}

func execute(t *testing.T, query string, primary bool, secondary bool, ignoreORA bool, dbaUser string) {
	if primary {
		resp := QueryOracle(t, query, "False", dbaUser)
		if strings.Contains(resp, "ORA-") && !ignoreORA {
			t.Fatalf("Failed while reading response %s", resp)
		}
	}

	if secondary {
		resp := QueryOracle(t, query, "True", dbaUser)
		if strings.Contains(resp, "ORA-") && !ignoreORA {
			t.Fatalf("Failed while reading response %s", resp)
		}
	}
}

func GiveROToPrimary(t *testing.T) {
	query := "GRANT CLOC_RW TO CLOCAPP;\\nREVOKE CLOC_RW FROM CLOCAPP;\\nGRANT CLOC_RO TO CLOCAPP"
	execute(t, query, true, false, false, "True")
	time.Sleep(10 * time.Second)
}

func GiveRWToPrimary(t *testing.T) {
	query := "GRANT CLOC_RO TO CLOCAPP;\\nREVOKE CLOC_RO FROM CLOCAPP;\\nGRANT CLOC_RW TO CLOCAPP"
	execute(t, query, true, false, false, "True")
	time.Sleep(10 * time.Second)
}

func GiveRWToSecondary(t *testing.T) {
	query := "GRANT CLOC_RO TO CLOCAPP;\\nREVOKE CLOC_RO FROM CLOCAPP;\\nGRANT CLOC_RW TO CLOCAPP"
	execute(t, query, false, true, false, "True")
	time.Sleep(10 * time.Second)
}

func GiveROToSecondary(t *testing.T) {
	query := "GRANT CLOC_RW TO CLOCAPP;\\nREVOKE CLOC_RW FROM CLOCAPP;\\nGRANT CLOC_RO TO CLOCAPP"
	execute(t, query, false, true, false, "True")
	time.Sleep(10 * time.Second)
}

func MoveCutOverPhase(t *testing.T, phase string, primary bool, secondary bool) {
	logger.GetLogger().Log(logger.Alert, "********************************")
	logger.GetLogger().Log(logger.Alert, "     "+phase+"       ")
	logger.GetLogger().Log(logger.Alert, "********************************")
	comment := t.Name()
	switch phase {

	case CleanCutOver:
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

	case CreateTable:
		query := "create table pypl_occ_cutover" +
			"(  DB_UNIQUE_NAME varchar2(64) NOT NULL," +
			"   OCC_NAME varchar2(32) NOT NULL," +
			"   PDB VARCHAR2(16) NOT NULL," +
			"   OCC_TNS_ALIAS varchar2(64) NOT NULL," +
			"   READ_STATUS char(3) NOT NULL," +
			"   WRITE_STATUS char(3) NOT NULL," +
			"   CUTOVER_PHASE varchar(16)  NOT NULL," +
			"   REMARKS varchar2(500)," +
			"   WISB_ROLES varchar(255)," +
			"   EXPIRATION_TIME DATE," +
			"   TNS_ALIAS_ROLE VARCHAR2(16)," +
			"   constraint \\\"check_read_status_chk\\\" CHECK (READ_STATUS IN ('Y','N','X', 'yes', 'no')) ENABLE," +
			"   constraint \\\"check_write_status_chk\\\" CHECK (WRITE_STATUS IN ('Y','N','X', 'yes', 'no' )) ENABLE," +
			"   constraint \\\"check_cutover_phase_chk\\\" CHECK (CUTOVER_PHASE IN ('ENABLE','FLEXUP','CUTOVER','NONE', 'invalid' )) ENABLE," +
			"   constraint \\\"check_tns_alias_role\\\" CHECK (TNS_ALIAS_ROLE IN ('src','tgt','SRC','TGT', 'invalid' )) ENABLE" +
			")"
		execute(t, query, primary, secondary, false, "True")

		query = "create unique index pypl_occ_cutover_pk on pypl_occ_cutover(DB_UNIQUE_NAME,OCC_NAME,OCC_TNS_ALIAS)"
		execute(t, query, primary, secondary, false, "True")

		query = "create index pypl_occ_cutover_dbun_idx on pypl_occ_cutover(db_unique_name)"
		execute(t, query, primary, secondary, false, "True")

		query = "create index pypl_occ_cutover_occname_idx on pypl_occ_cutover(occ_name)"
		execute(t, query, primary, secondary, false, "True")

		query = "grant all on pypl_occ_cutover to clocapp"
		execute(t, query, primary, secondary, false, "True")

		break

	case CutOverEnable:

		GiveRWToPrimary(t)
		GiveROToSecondary(t)

		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'HERABOX', 'CLOC', 'Y', 'Y', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'HERABOX', 'CLOC_STG1', 'N', 'N', 'ENABLE', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case FlexUp:
		GiveRWToPrimary(t)
		query := "update pypl_occ_cutover set remarks='" + comment +
			"', wisb_roles='CLOC_RW', write_status='Y' where db_unique_name='HERADB_ONE' and occ_name='occ';\\n"
		execute(t, query, primary, secondary, false, "False")
		time.Sleep(3 * time.Second)
		query = "update pypl_occ_cutover set cutover_phase='FLEXUP', remarks='" + comment +
			"', write_status='Y' where db_unique_name='HERADB_ONE' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='FLEXUP', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ'\\n"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverPhaseI:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='Y', write_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='N', write_status='N', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseAlone:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseII:
		GiveROToSecondary(t)
		query := "update pypl_occ_cutover set write_status='N', read_status='N', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set write_status='N', read_status='Y', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIII:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='N', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case SourceTargetFlip:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case FlipEnable:
		query := "update pypl_occ_cutover set cutover_phase='ENABLE', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='ENABLE', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIWithoutRole:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='N', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case DeleteCutOverTable:
		query := "drop table pypl_occ_cutover"
		execute(t, query, primary, secondary, true, "True")
		break

	case CutOverEnableWrongOCC:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)

		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ-wrong', 'HERABOX', 'CLOC', 'yes', 'yes', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ-wrong', 'HERABOX', 'CLOC_STG1', 'N', 'N', 'ENABLE', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverEnableInvalidUniqueName:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE_INVALID', 'occ', 'HERABOX', 'CLOC', 'yes', 'yes', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO_INVALID', 'occ', 'HERABOX', 'CLOC_STG1', 'no', 'no', 'ENABLE', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverEnableInvalidTNS:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'HERABOX', 'CLOC_INVALID', 'yes', 'yes', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'HERABOX', 'CLOC_STG1_INVALID', 'no', 'no', 'ENABLE', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverEnableInvalidPhase:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'HERABOX', 'CLOC', 'yes', 'yes', 'invalid', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'HERABOX', 'CLOC_STG1', 'no', 'no', 'invalid', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverEnableInvalidNumRows:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'HERABOX', 'CLOC', 'yes', 'yes', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverEnableInvalidRead:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'HERABOX', 'CLOC', 'X', 'yes', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'HERABOX', 'CLOC_STG1', 'X', 'no', 'ENABLE', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverEnableInvalidWrite:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'HERABOX', 'CLOC', 'yes', 'X', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'HERABOX', 'CLOC_STG1', 'no', 'X', 'ENABLE', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverEnableWriteNoRead:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'HERABOX', 'CLOC', 'no', 'yes', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'HERABOX', 'CLOC_STG1', 'no', 'no', 'ENABLE', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverEnableDualWrite:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'HERABOX', 'CLOC', 'yes', 'yes', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'HERABOX', 'CLOC_STG1', 'no', 'yes', 'ENABLE', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverEnableDualRead:
		GiveRWToPrimary(t)
		GiveROToSecondary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'HERABOX', 'CLOC', 'yes', 'yes', 'ENABLE', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'HERABOX', 'CLOC_STG1', 'yes', 'no', 'ENABLE', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")

		break

	case CutOverFlexUpInValidUniqName:
		GiveRWToPrimary(t)
		query := "delete from pypl_occ_cutover"
		execute(t, query, primary, secondary, false, "False")

		query = "insert into pypl_occ_cutover values ('HERADB_ONE_INVALID', 'occ', 'HERABOX', 'CLOC', 'yes', 'yes', 'FLEXUP', '" +
			comment + "', 'CLOC_RW', '', 'SRC');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO_INVALID', 'occ', 'HERABOX', 'CLOC_STG1', 'no', 'no', 'FLEXUP', '" +
			comment + "', 'CLOC_RO', '', 'TGT')"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIInvalidRowCount:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ';\\n" +
			"delete from pypl_occ_cutover where db_unique_name='HERADB_TWO' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIInvalidUniqName:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', db_unique_name='HERADB_ONE_INVALID', write_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', db_unique_name='HERADB_TWO_INVALID', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break
	case CutOverPhaseICorrectUniqName:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', db_unique_name='HERADB_ONE', write_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', db_unique_name='HERADB_TWO', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break
	case CutOverPhaseIInvalidOCCName:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', occ_name='occ-invalid', write_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', occ_name='occ-invalid', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIInvalidPhase:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='invalid', write_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='invalid', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIDualWrite:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='Y', remarks='" + comment +
			"';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='Y', remarks='" + comment +
			"'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIDualRead:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='Y', wisb_roles='CLOC_RO', remarks='" + comment +
			"';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='Y', wisb_roles='CLOC_RO', remarks='" + comment +
			"'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIReadOff:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIInvalidWriteStatus:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='X', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='X', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break
	case CutOverPhaseIInvalidReadStatus:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='X', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='X', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIInvalidTwoTask:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', OCC_TNS_ALIAS='TWO_TASK_INVALID', write_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', OCC_TNS_ALIAS='TWO_TASK_INVALID', write_status='N', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIInvalidRowCount:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='N', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ';\\n" +
			"delete from pypl_occ_cutover where db_unique_name='HERADB_TWO' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIInvalidDBUniqName:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', db_unique_name='HERADB_ONE_INVALID', read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', db_unique_name='HERADB_TWO_INVALID', read_status='Y', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIInvalidOCCName:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', occ_name='occ-invalid', read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', occ_name='occ-invalid', read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIInvalidPhase:
		query := "update pypl_occ_cutover set cutover_phase='invalid', read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='invalid', read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIInvalidTwoTask:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', OCC_TNS_ALIAS='TWO_TASK_INVALID', read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', OCC_TNS_ALIAS='TWO_TASK_INVALID', read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIInvalidRowCount:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"delete from pypl_occ_cutover where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIInvalidDBUniqName:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', db_unique_name='HERADB_ONE_INVALID', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', db_unique_name='HERADB_TWO_INVALID', write_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIInvalidOCCName:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', occ_name='occ-invalid', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', occ_name='occ-invalid', write_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIReadOff:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set read_status='N', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIDualRead:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set read_status='Y', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set read_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIDualWrite:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set write_status='Y', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set write_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIInvalidWrite:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set write_status='X', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set write_status='X', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIInvalidPhase:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set cutover_phase='invalid', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='invalid', write_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIInvalidRead:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set read_status='X', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set read_status='X', write_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverPhaseIIIInvalidTwoTask:
		GiveRWToSecondary(t)
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', OCC_TNS_ALIAS='TWO_TASK_INVALID', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', OCC_TNS_ALIAS='TWO_TASK_INVALID', write_status='Y', wisb_roles='CLOC_RW', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseInvalidRowCount:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		query = "update pypl_occ_cutover set cutover_phase='ENABLE', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"delete from pypl_occ_cutover where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseInvalidDBUniqName:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")

		query = "update pypl_occ_cutover set cutover_phase='ENABLE', db_unique_name='HERADB_ONE_INVALID',  remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='ENABLE', db_unique_name='HERADB_TWO_INVALID',  remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseInvalidOCCName:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		query = "update pypl_occ_cutover set cutover_phase='ENABLE', occ_name='occ-invalid', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='ENABLE', occ_name='occ-invalid', write_status='Y', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseReadOff:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		query = "update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseDualRead:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		query = "update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='Y', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='Y', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseDualWrite:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		query = "update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='Y', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='Y', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseInvalidWrite:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		query = "update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='X', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='X', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseInvalidRead:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		query = "update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='X', write_status='N', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', read_status='X', write_status='Y', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseInvalidPhase:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		query = "update pypl_occ_cutover set cutover_phase='invalid', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='invalid', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverCompletePhaseInvalidTwoTask:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='SRC', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', TNS_ALIAS_ROLE='TGT', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		query = "update pypl_occ_cutover set cutover_phase='ENABLE', OCC_TNS_ALIAS='TWO_TASK_INVALID', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='ENABLE', OCC_TNS_ALIAS='TWO_TASK_INVALID', remarks='" + comment +
			"' where OCC_TNS_ALIAS='CLOC_STG1' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	case CutOverFlexUpInCorrectRole:
		GiveROToPrimary(t)
		query := "update pypl_occ_cutover set cutover_phase='FLEXUP', read_status='Y', write_status='Y', wisb_roles='CLOC_RO', remarks='" + comment +
			"' where db_unique_name='HERADB_ONE' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='FLEXUP', read_status='N', write_status='N', remarks='" + comment +
			"' where db_unique_name='HERADB_TWO' and occ_name='occ'"
		execute(t, query, primary, secondary, false, "False")
		break

	default:
		t.Fatalf("Unknown cutover phase %s.\n", phase)
	}
}

func OCCConfig(t *testing.T, key string, value string, filename string) {
	logger.GetLogger().Log(logger.Alert, "Changing occ config in file ", filename,
		" : key: ", key, ", value: ", value)
	url := "http://" + heraBoxHost + ":8000/occ/occ_config?key=" + key + "&value=" + value + "&filename=" + filename
	response := httpGet(t, url)

	_, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func OCCBinarySetup(t *testing.T, filePath string, filename string) {
	if _, exists := binaryPushDone[filePath]; exists {
		return
	}
	binaryPushDone[filename] = true
	logger.GetLogger().Log(logger.Alert, "Pushing ", filename, " binary from ", filePath)
	client := &http.Client{
		Timeout: time.Second * 60,
	}

	url := "http://" + heraBoxHost + ":8000?filename=" + filename
	b, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf(err.Error())
	}

	retryCount := 0

	for {
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(b))
		if err != nil {
			t.Fatalf(err.Error())
		}

		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := client.Do(req)
		if resp == nil {
			if retryCount >= maxRetryCount {
				t.Fatalf("unable to push the binary to Herabox setup")
			}
			retryCount += 1
			logger.GetLogger().Log(logger.Alert, "sleeping 5 sec before retrying ", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf(strconv.Itoa(resp.StatusCode))
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Failed while reading response %s", err)
		}
		if strings.Contains(string(body), "ORA-") {
			t.Fatalf("Failed while reading response %s", body)
		}
		break
	}
}

func StopOCCDocker(t *testing.T) {
	logger.GetLogger().Log(logger.Alert, "Stopping occ docker")
	url := "http://" + heraBoxHost + ":8000/docker_support?container=occ&action=stop"
	response := httpGet(t, url)

	_, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func StartOCCDocker(t *testing.T) {
	logger.GetLogger().Log(logger.Alert, "Starting occ docker")
	url := "http://" + heraBoxHost + ":8000/docker_support?container=occ&action=start"
	response := httpGet(t, url)

	_, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func IsContainerUp(t *testing.T, name string) bool {
	logger.GetLogger().Log(logger.Alert, "Checking Docker Status of ", name)
	url := "http://" + heraBoxHost + ":8000/docker_support?container=" + name + "&action=status"
	response := httpGet(t, url)

	resp, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if strings.TrimSpace(string(resp)) == "true" {
		return true
	}

	return false
}

func RestartOCC(t *testing.T, timeToSleep int) int64 {
	logger.GetLogger().Log(logger.Alert, "restarting occ")
	url := "http://" + heraBoxHost + ":8000/occ/restart_occ"
	response := httpGet(t, url)

	_, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
	logger.GetLogger().Log(logger.Alert, "Sleeping for ", timeToSleep, " seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(time.Duration(timeToSleep) * time.Second)
	return afterRestart
}

func QueryOracle(t *testing.T, query string, cutOver string, dbaUser string) string {
	url := "http://" + heraBoxHost + ":8000/run_query?cut_over=" + cutOver + "&dba_user=" + dbaUser
	jsonStr := "{\"query\":\"" + query + "\"" +
		", \"dba_user\": \"" + dbaUser + "\",\"cut_over\":\"" + cutOver + "\"}"

	logger.GetLogger().Log(logger.Alert, "Running SQL: ", strings.Replace(query, strconv.Itoa(int('"')), "'", -1),
		"cutOver=", cutOver)
	counter := 1
	for {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsonStr)))
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			if counter > maxRetryCount {
				t.Fatalf("Failed while calling %s", err)
			}
			logger.GetLogger().Log(logger.Alert, "Retry as failed ", counter, err)
			counter += 1
			continue
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			if counter > maxRetryCount {
				t.Fatalf("Failed while reading response %s", err)
			}
			logger.GetLogger().Log(logger.Alert, "Retry as failed ", counter, err)
			counter += 1
			continue
		}

		return string(body)
	}
}

func validateServiceStatus(dbStatus []DBStatus, dbUniqueName string, serviceName string, status string) bool {
	foundActiveService := false
	for _, db := range dbStatus {
		for _, service := range db.DatabaseServices {
			if strings.TrimSpace(db.DBUniqueName) == dbUniqueName &&
				strings.TrimSpace(service.ServiceName) == serviceName && service.Active == true {
				foundActiveService = true
				break
			}
		}
	}
	if foundActiveService && status == "stop_service" {
		return false
	}
	if !foundActiveService && status == "start_service" {
		return false
	}
	return true
}

func LockUnlockUser(t *testing.T, action string, cutOver bool) []DBStatus {
	var status []DBStatus
	co := "False"
	if cutOver {
		co = "True"
		logger.GetLogger().Log(logger.Alert, action, " user on cutover db")
	} else {
		logger.GetLogger().Log(logger.Alert, action, " user on main db")
	}
	url := "http://" + heraBoxHost + ":8000/occ/lock_user?cut_over=" + co
	if action == "unlock" {
		url = "http://" + heraBoxHost + ":8000/occ/unlock_user?cut_over=" + co
	}
	response := httpGet(t, url)

	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = json.Unmarshal(responseData, &status)
	if err != nil {
		t.Fatalf(err.Error())
	}
	return status
}

func DefaultTns(t *testing.T) {
	logger.GetLogger().Log(logger.Alert, "Move TNS to Default Value")
	response := httpGet(t, "http://"+heraBoxHost+":8000/\"default_tns\"")

	_, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

// EnableCutOver /*
/*
 1. stop occ docker
 2. push tns with cut over details
 3. enable cut over env variable for occ
 4. start occ docker
 5. copy the new mux binary
 6. disable/enable read/write split
*/
func EnableCutOver(t *testing.T, enableRWSplit bool, enableShard bool) {
	logger.GetLogger().Log(logger.Alert, "EnableCutOver")
	url := "http://" + heraBoxHost + ":8000/enable_cut_over"
	if enableShard {
		url += "?shard=True"
	}
	if enableRWSplit {
		url += "?read_write=True"
	}
	response := httpGet(t, url)

	_, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !enableRWSplit {
		OCCConfig(t, "readonly_children_pct", "0", "/x/web/LIVE/occ/occ.cdb")
	}
	OCCConfig(t, "enable_cutover", "true", "/x/web/LIVE/occ/occ.cdb")
	//EnableDebugLog(t)
}

func EnableDebugLog(t *testing.T) {
	OCCConfig(t, "log_level", "4", "/x/web/LIVE/occ/occ.cdb")
	OCCConfig(t, "log_level", "4", "/x/web/LIVE/occ/hera.txt")
	OCCConfig(t, "opscfg.occ.server.log_level", "4", "/x/web/LIVE/opscfg/occ.cdb")
}

func EnableSharding(t *testing.T) {
	OCCConfig(t, "enable_sharding", "true", "/x/web/LIVE/occ/occ.cdb")
	OCCConfig(t, "num_shards", "2", "/x/web/LIVE/occ/occ.cdb")
	OCCConfig(t, "shard_key_name", "id", "/x/web/LIVE/occ/occ.cdb")
	//OCCConfig(t, "sharding_algo", "hash", "/x/web/LIVE/occ/occ.cdb")
	OCCConfig(t, "management_table_prefix", "pypl_occ", "/x/web/LIVE/occ/occ.cdb")

}

// ResetOCCDocker /**
/*
1. Stop OCC Docker container
2. Push default TNS File (without cutover)
3. Disable CLOC_STG1 env variable to disable cutover
4. Start OCC Docker
*/
func ResetOCCDocker(t *testing.T) {
	logger.GetLogger().Log(logger.Alert, "ResetOCCDocker")
	response := httpGet(t, "http://"+heraBoxHost+":8000/reset")

	_, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}

}

func KillSessionAndValidate(t *testing.T, stateLog map[string]int, serviceName string, cutover bool) {
	cnt := 0
	for {
		if ValidateStateLog(t, stateLog, false) || cnt > 2 {
			break
		}
		KillSessions(t, cutover, serviceName)
		logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
		cnt += 1
	}

}

func httpGet(t *testing.T, url string) *http.Response {
	retryCount := 0
	for {
		response, err := http.Get(url)
		if err != nil {
			retryCount += 1
			if retryCount > maxRetryCount {
				if t == nil {
					panic(err)
				} else {
					t.Fatalf(err.Error())
				}
			}
			logger.GetLogger().Log(logger.Alert, "sleeping 5 sec and retrying - failed on http get ", err)
			time.Sleep(5 * time.Second)
		} else {
			return response
		}
	}
}

func KillSessions(t *testing.T, cutOver bool, serviceName string) []DBStatus {
	var status []DBStatus

	co := "False"
	if cutOver {
		logger.GetLogger().Log(logger.Alert, "Kill Session for cutover db")
		co = "True"
	} else {
		logger.GetLogger().Log(logger.Alert, "Kill Session for main db")
	}
	response := httpGet(t, "http://"+heraBoxHost+":8000/occ/kill_session?cut_over="+co+"&service_name="+serviceName)
	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = json.Unmarshal(responseData, &status)
	if err != nil {
		t.Fatalf(err.Error())
	}
	return status
}

func ShutDownDBService(dbUniqueName string, serviceName string, t *testing.T) {
	_, activeResponse := GetDBStatus()
	if !activeResponse[dbUniqueName][serviceName] {
		logger.GetLogger().Log(logger.Alert, dbUniqueName, " : ", serviceName, "is already in down state")
		return
	}
	action := "stop_service"
	logger.GetLogger().Log(logger.Alert, "shutting down", dbUniqueName, " : ", serviceName)
	dbStatus := dbService(dbUniqueName, serviceName, action)

	if validateServiceStatus(dbStatus, dbUniqueName, serviceName, action) != true {
		t.Fatalf("Failed " + action + ":" + dbUniqueName + ":" + serviceName)
	}
}

func StartDBService(dbUniqueName string, serviceName string, t *testing.T) {
	_, activeResponse := GetDBStatus()
	if activeResponse[dbUniqueName][serviceName] {
		logger.GetLogger().Log(logger.Alert, dbUniqueName, " : ", serviceName, "is already in up state")
		return
	}
	action := "start_service"
	logger.GetLogger().Log(logger.Alert, "starting ", dbUniqueName, ":", serviceName)
	dbStatus := dbService(dbUniqueName, serviceName, action)

	if validateServiceStatus(dbStatus, dbUniqueName, serviceName, action) != true {
		t.Fatalf("Failed " + action + ":" + dbUniqueName + ":" + serviceName)
	}
}

func GetDBStatus() ([]DBStatus, map[string]map[string]bool) {
	var status []DBStatus

	activeResponse := make(map[string]map[string]bool)
	response := httpGet(nil, "http://"+heraBoxHost+":8000/occ/status_from_db")

	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(responseData, &status)
	if err != nil {
		panic(err)
	}

	for _, db := range status {
		for _, service := range db.DatabaseServices {
			un := strings.TrimSpace(db.DBUniqueName)
			sn := strings.TrimSpace(service.ServiceName)
			_, ok := activeResponse[un]
			if !ok {
				activeResponse[un] = make(map[string]bool)
			}
			activeResponse[un][sn] = service.Active
		}
	}

	return status, activeResponse
}

func rollbackTxn(txn *DBTxn) {
	defer txn.DBConnection.Close()
	err := txn.DBTransaction.Rollback()
	if err != nil {
		return
	}
}

func commitTxn(txn *DBTxn) error {
	defer txn.DBConnection.Close()
	return txn.DBTransaction.Commit()
}

func exec(conn *sql.Conn, ctx context.Context, query string) *sql.Rows {
	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		panic(err)
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		panic(err)
	}
	return rows
}

func slowQuery(txn *sql.Tx, expectedId int, sec int, wg *sync.WaitGroup) {
	defer wg.Done()
	query := fmt.Sprintf("select SLOW_QUERY(%d) from dual", sec)
	rows, err := txn.Query(query)

	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			panic(err)
		}
		if expectedId != id {
			msg := fmt.Sprintf("Wrong DB connected expected %d but got %d", expectedId, id)
			panic(msg)
		}
	}
}

func ValidateReadWorkerID(ReadWaitTime int, expectedId int, wg *sync.WaitGroup) {
	c := TestConnection{}
	c.GetConnection()
	if c.Err != nil {
		panic(c.conn)
	}
	defer c.Close()
	txn, err := c.conn.BeginTx(c.context, nil)
	if err != nil {
		panic(err)
	}
	wg.Add(1)
	go slowQuery(txn, expectedId, ReadWaitTime, wg)
}

func writeBeginTxn() (*DBTxn, error) {
	c := TestConnection{}
	c.GetConnection()
	if c.Err != nil {
		c.Close()
		return nil, c.Err
	}
	txn, err := c.conn.BeginTx(c.context, nil)
	if err != nil {
		txn.Rollback()
		return nil, err
	}
	insertQuery := "insert into occ_test values(id_seq.NEXTVAL, 'hold-record', 1)"
	_, err = txn.ExecContext(c.context, insertQuery)
	if err != nil {
		txn.Rollback()
		c.Close()
		return nil, err
	}

	return &DBTxn{DBConnection: c, DBTransaction: txn}, nil
}

func validateDBID(dbTxn *DBTxn, dbId int, dbName string) {

	query := "select * from db_id_test"
	stmt, err := dbTxn.DBTransaction.PrepareContext(dbTxn.DBConnection.context, query)
	if err != nil {
		panic(err)
	}
	rows, err := stmt.QueryContext(dbTxn.DBConnection.context)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var id int
		if err := rows.Scan(&id, &name); err != nil {
			panic(err)
		}
		if id != dbId || name != dbName {
			panic("Wrong DB ID present")
		}
	}
}

func SortStats(trafficStats map[int64]ClientTrafficStats) []int64 {
	keys := make([]int64, 0)
	for k, _ := range trafficStats {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func ValidateFailureTraffic(t *testing.T, trafficStats map[int64]ClientTrafficStats, queryType string,
	startTime int64, endTime int64) {
	logger.GetLogger().Log(logger.Alert, "Validating Failure traffic for ", queryType, " from ", startTime, " to ", endTime)
	for _, utc := range SortStats(trafficStats) {

		if utc < startTime || utc > endTime {
			continue
		}

		cts := trafficStats[utc]

		for dbId := 1; dbId < 3; dbId++ {
			if cts.stats[queryType][dbId].successCount != 0 {
				CT.DumpStats(trafficStats)
				t.Fatalf("UTC: %d, Expected %s Query to fail but found in db %d", utc, queryType, dbId)
			}
		}

		if cts.stats[queryType][0].failureCount == 0 {
			CT.DumpStats(trafficStats)
			t.Fatalf("UTC: %d, Expected failure for %s - but no failure found", utc, queryType)
		}

	}
}

func ValidateSuccessTraffic(t *testing.T, trafficStats map[int64]ClientTrafficStats, queryType string,
	startTime int64, endTime int64, dbIdWithTraffic int, dbIdNoTraffic int) {

	logger.GetLogger().Log(logger.Alert, "Validating Success traffic for ", queryType, " in DB:", dbIdWithTraffic, "from ", startTime, " to ", endTime)
	for _, utc := range SortStats(trafficStats) {

		if utc < startTime || utc > endTime {
			continue
		}

		cts := trafficStats[utc]
		if cts.stats[queryType][dbIdWithTraffic].successCount == 0 {
			CT.DumpStats(trafficStats)
			t.Fatalf("UTC: %d, Expected %s Query in DB: %d - but not found", utc, queryType, dbIdWithTraffic)
		}

		if cts.stats[queryType][dbIdWithTraffic].failureCount != 0 {
			CT.DumpStats(trafficStats)
			t.Fatalf("UTC: %d, Expected %s Query to succeed in DB: %d - but failed", utc, queryType, dbIdWithTraffic)
		}

		if cts.stats[queryType][dbIdNoTraffic].successCount != 0 {
			CT.DumpStats(trafficStats)
			t.Fatalf("UTC: %d, Not expecting %s Query in DB: %d - but found", utc, queryType, dbIdWithTraffic)
		}

		if cts.stats[queryType][dbIdNoTraffic].failureCount != 0 {
			CT.DumpStats(trafficStats)
			t.Fatalf("UTC: %d, Not expecting %s Query in DB: %d - but found as failures", utc, queryType, dbIdWithTraffic)
		}

		if cts.stats[queryType][1].failureCount > 0 || cts.stats[queryType][2].failureCount > 0 {
			CT.DumpStats(trafficStats)
			t.Fatalf("UTC: %d, Expected No %s failure. DB1: %d failed, DB2: %d failed", utc, queryType,
				cts.stats[queryType][1].failureCount, cts.stats[queryType][2].failureCount)
		}
	}
}

func ValidateConnectionIntegrity(WriteWorkerCount int, ReadWorkerCount int, ReadWaitSeconds int,
	expectedId int, expectedDBString string) {
	var dbWriteTrans []*DBTxn
	var wg sync.WaitGroup

	logger.GetLogger().Log(logger.Alert, " and as of now it is connected to db_1")

	logger.GetLogger().Log(logger.Alert, "Validating occ connection integrity: Validating WriteWorker:", WriteWorkerCount, ", Validating ReadWorker:", ReadWorkerCount)

	for i := 0; i < WriteWorkerCount; i++ {
		txn, _ := writeBeginTxn()
		dbWriteTrans = append(dbWriteTrans, txn)
	}

	for i := 0; i < ReadWorkerCount; i++ {
		ValidateReadWorkerID(ReadWaitSeconds, expectedId, &wg)
	}

	for _, dbTxn := range dbWriteTrans {
		validateDBID(dbTxn, expectedId, expectedDBString)
	}

	// wait for readers as they are blocked by procedure
	wg.Wait()

	for _, dbTxn := range dbWriteTrans {
		rollbackTxn(dbTxn)
	}

}

func ValidateWorkerCountFromDatabase(dbUniqueName string, serviceName string, serviceActive bool,
	expectedWorkerCount int, t *testing.T) {
	logger.GetLogger().Log(logger.Alert, "Validating ", dbUniqueName, ":", serviceName,
		" has ", expectedWorkerCount, " workers connected to db")
	retryCount := 0
	retry := false
	validationSuccess := false
	workerFound := false

	// retry in case of temp failures
	for {
		validationSuccess = false
		retry = false
		workerFound = false
		// load status from database
		databaseServices, _ := GetDBStatus()

		// for each database
		for _, db := range databaseServices {
			// for each database service
			for _, service := range db.DatabaseServices {
				if strings.TrimSpace(db.DBUniqueName) == dbUniqueName &&
					strings.TrimSpace(service.ServiceName) == serviceName && service.Active == serviceActive {
					workerFound = true
					if service.WorkerCount != expectedWorkerCount {
						if retryCount >= maxRetryCount {
							t.Fatalf("Failed for worker count in %s:%s - expected/actual %d/%d",
								strings.TrimSpace(db.DBUniqueName),
								strings.TrimSpace(service.ServiceName),
								expectedWorkerCount, service.WorkerCount)
						} else {
							retry = true
							logger.GetLogger().Log(logger.Alert, "Retry: Failed for worker count in ", strings.TrimSpace(db.DBUniqueName),
								":", strings.TrimSpace(service.ServiceName), " - expected/actual ",
								expectedWorkerCount, "/", service.WorkerCount)
							break
						}
					} else {
						validationSuccess = true
						break
					}
				}
			}
			if retry || validationSuccess {
				break
			}
		}

		if retryCount >= maxRetryCount || validationSuccess || expectedWorkerCount == -1 {
			break
		}
		retryCount += 1
		logger.GetLogger().Log(logger.Alert, "Sleeping for 8 seconds and retrying validation")
		time.Sleep(8 * time.Second)
	}
	if expectedWorkerCount >= 0 && !validationSuccess {
		t.Fatalf("Failed for worker count in %s:%s, expectedWorkerCount: %d", strings.TrimSpace(dbUniqueName),
			strings.TrimSpace(serviceName), expectedWorkerCount)
	}

	if expectedWorkerCount == -1 && workerFound {
		t.Fatalf("Service should not be up and running %s:%s", strings.TrimSpace(dbUniqueName),
			strings.TrimSpace(serviceName))
	}
}
