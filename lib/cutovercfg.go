package lib

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
)

// each record represents a table's row of record
type CutoverRecord struct {
	moduleName   sql.NullString
	tnsAliasRole sql.NullString
	dbUname      sql.NullString
	occTnsAlias  sql.NullString
	wstatus      sql.NullString
	rstatus      sql.NullString
	phase        sql.NullString
	wisbRoles    sql.NullString
}

var gModuleName string

// 2task names once initialized will never change
var gTnsAlias string         // e.g.  MONEY
var gTnsAliasCutover string  // e.g.  MONEY_CUTOVER
var gTnsAliasR string        //e.g. MONEY_OCC
var gTnsAliasCutoverR string // e.g. MONEY_OCC_CUTOVER

// a comphrehensive version of the state
type CutoverCfg struct {
	Phase         string            // current cutover phase
	ActiveTns     string            // FOO or FOO_CUTOVER is the active. If no active, set to UnsetStr ("NONE")
	ActiveShardId ShardByTwoTask    // active shard id mapped to FOO or FOO_CUTOVER. Set to -1 if no active.
	TnsByRole     map[string]string // two_task or two_task_cutover by db role 'SRC' or 'TGT'
	DbByTns       map[string]string // DB_UNAME by two_task and two_task_cutover
	RWstatusByDb  map[string]int    // unique db name --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
	UserRoleByDb  map[string]int    // unique db name --> user enabled role.
}

// we will have to view the records atomically.
var gCutoverCfg atomic.Value

func GetTnsName() string {
	return gTnsAlias
}

func GetTnsCutoverName() string {
	return gTnsAliasCutover
}

func GetTnsRname() string {
	return gTnsAliasR
}

func GetTnsRcutoverName() string {
	return gTnsAliasCutoverR
}

// Get the cfg atomically
func GetCutoverCfg() CutoverCfg {
	cfg := gCutoverCfg.Load()
	if cfg == nil {
		return CutoverCfg{Phase: ""}
	}
	return cfg.(CutoverCfg)
}

// main.go calls this function. DB can't suspend user sessions.
func InitCutoverCfg(modulename string) error {
	if !GetConfig().EnableCutover {
		return nil
	}

	startData := make(chan *CutoverCfg)
	gModuleName = strings.ToUpper(modulename)
	for s := 0; s < int(MaxDbInCutover); s++ {
		x := s
		go func() {
			shid := ShardByTwoTask(x)
			i := 0
			var db *sql.DB
			var err error
			maxRetry := 30
			for ; i < maxRetry; i++ {
				if db != nil {
					db.Close()
				}
				// as initialization, it needs to hit two connection pool and compare.
				db, err = cutoverOpenDb(shid)
				retryInfo := strconv.Itoa(int(shid)) + "_" + strconv.Itoa(i)
				if err != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "err_init_opendb_"+retryInfo, cal.TransOK, err.Error())
					evt.Completed()
					logger.GetLogger().Log(logger.Warning, "error: cutoverOpenDb() shard", retryInfo, err.Error())
				} else {
					tmpcfg, err := loadCutoverCfg(db, true)
					if err != nil {
						evt := cal.NewCalEvent(EvtTypeCutover, "err_init_loadcfg_"+retryInfo, cal.TransOK, err.Error())
						evt.Completed()
						logger.GetLogger().Log(logger.Warning, "error: loadCutoverCfg()", retryInfo, err.Error())
					} else {
						evt := cal.NewCalEvent(EvtTypeCutover, "init_success_"+retryInfo, cal.TransOK, "")
						evt.Completed()
						logger.GetLogger().Log(logger.Warning, "successful init cutovercfg sh:", shid, ", ", tmpcfg)
						startData <- &tmpcfg
						break // break the inner retry loop
					}
				}
				time.Sleep(time.Second)
			}

			if i == maxRetry {
				evt := cal.NewCalEvent(EvtTypeCutover, "init_fail_max_retry_"+strconv.Itoa(int(shid)), cal.TransOK, "")
				evt.Completed()
				var emptycfg CutoverCfg
				startData <- &emptycfg
			}
		}()
	}

	cnt := 0
	shutdown := false
	var firstcfg [2]CutoverCfg // wait for exact two row of records
	for {
		if cnt >= 2 {
			break // for loop
		}
		select {
		case peekcfg := <-startData:
			if len(peekcfg.TnsByRole) == 0 {
				logger.GetLogger().Log(logger.Warning, "empty cutover cfg, abort start up")
				shutdown = true
				break
			}
			copyCutoverCfg(&firstcfg[cnt], peekcfg)
		case <-time.After(time.Minute):
			logger.GetLogger().Log(logger.Warning, "timed out waiting on the init cfg", cnt)
			shutdown = true
		}
		cnt++
	}

	rc := 0
	changed := false
	if !shutdown {
		changed, rc = CheckCfgChange(firstcfg[0], firstcfg[1])
		if changed {
			shutdown = true
		}
	}

	if shutdown {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_initcfg", cal.TransOK, strconv.Itoa(rc))
		evt.Completed()
		errmsg := fmt.Sprintf("shutdown due to incorrect/inconsistent cutover cfg during init %d", rc)
		return errors.New(errmsg)
	}
	initUpdateGlobalCfg(&firstcfg[0])
	logger.GetLogger().Log(logger.Warning, "successful cutovercfg init at start up", GetCutoverCfg())
	hostname, _ := os.Hostname()
	go func() {
		var db *sql.DB
		var err error
		for {
			time.Sleep(time.Second * time.Duration(GetConfig().CutoverCfgReloadInterval))
			if db != nil {
				db.Close()
			}

			// always two_task connections
			db, err = cutoverOpenDb(ShIdUnset)
			if err != nil {
				evt := cal.NewCalEvent(EvtTypeCutover, "err_reload_opendb", cal.TransOK, err.Error())
				evt.Completed()
				logger.GetLogger().Log(logger.Warning, "err_reload_opendb", err.Error())
			} else {
				_, err = loadCutoverCfg(db, false)
				if err != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "err_reload", cal.TransOK, err.Error())
					evt.Completed()
					logger.GetLogger().Log(logger.Warning, "error: reload loadCutoverCfg()", err.Error())
				} else {
					evt := cal.NewCalEvent(EvtTypeCutover, "cfg_reloaded", cal.TransOK, hostname)
					evt.Completed()
					logger.GetLogger().Log(logger.Info, "successful reload cutovercfg")
				}
			}
		}
	}()
	return nil
}

// Get the SQL used to read the cutover configuration.
func getCutoverSQL() string {
	sqltxt := fmt.Sprintf("select /* cutover cfg */ upper(occ_name), upper(tns_alias_role), upper(db_unique_name), upper(occ_tns_alias), upper(cutover_phase), upper(write_status), upper(read_status), upper(wisb_roles) from %s_cutover where upper(occ_name) = upper('%s') and upper(occ_tns_alias) IN (upper('%s'), upper('%s'))",
		GetConfig().ManagementTablePrefix,
		//GetConfig().CutoverPostfix, // why do we need postfix for table name ?
		gModuleName,
		gTnsAlias,
		gTnsAliasCutover)
	return sqltxt
}

/*
Phase         string            // current cutover phase
ActiveTns     string            // FOO or FOO_CUTOVER is the active. If no active, set to UnsetStr ("NONE")
ActiveShardId ShardByTwoTask    // active shard id mapped to FOO or FOO_CUTOVER. Set to -1 if no active.
TnsByRole     map[string]string // two_task or two_task_cutover by db role 'SRC' or 'TGT'
DbByTns       map[string]string // DB_UNAME by two_task and two_task_cutover
RWstatusByDb  map[string]int    // unique db name --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
UserRoleByDb  map[string]int    // unique db name --> user enabled role.
*/
func compInitCfg(rec1 *CutoverCfg, rec2 *CutoverCfg) bool {
	same := true
	if (rec1.ActiveShardId != rec2.ActiveShardId) || (rec1.ActiveTns != rec2.ActiveTns) || (rec1.Phase != rec2.Phase) {
		logger.GetLogger().Log(logger.Alert, "cutovercfg init inconsistent Active or Phase")
		same = false
	}
	if !reflect.DeepEqual(rec1.TnsByRole, rec2.TnsByRole) {
		logger.GetLogger().Log(logger.Alert, "cutovercfg init inconsistent TnsByRole")
		same = false
	}
	if !reflect.DeepEqual(rec1.DbByTns, rec1.DbByTns) {
		logger.GetLogger().Log(logger.Alert, "cutovercfg init inconsistent DbByTns")
		same = false
	}

	if !reflect.DeepEqual(rec1.RWstatusByDb, rec2.RWstatusByDb) {
		logger.GetLogger().Log(logger.Alert, "cutovercfg init inconsistent RWstatusByDb")
		same = false
	}

	if !reflect.DeepEqual(rec1.UserRoleByDb, rec2.UserRoleByDb) {
		logger.GetLogger().Log(logger.Alert, "cutovercfg init inconsistent UserRoleByDb")
		same = false
	}
	return same
}

// this function create a deep copy from src to dst
func copyCutoverCfg(dst *CutoverCfg, src *CutoverCfg) {
	dst.Phase = src.Phase
	dst.ActiveTns = src.ActiveTns
	dst.ActiveShardId = src.ActiveShardId
	if dst.TnsByRole == nil {
		dst.TnsByRole = make(map[string]string, 3)
	}
	for t := range src.TnsByRole {
		dst.TnsByRole[t] = src.TnsByRole[t]
	}
	if dst.DbByTns == nil {
		dst.DbByTns = make(map[string]string, 3)
	}
	for t := range src.DbByTns {
		dst.DbByTns[t] = src.DbByTns[t]
	}
	dst.RWstatusByDb = src.RWstatusByDb
	for d := range src.RWstatusByDb {
		dst.RWstatusByDb[d] = src.RWstatusByDb[d]
	}
	dst.UserRoleByDb = src.UserRoleByDb
}

func initUpdateGlobalCfg(newcfg *CutoverCfg) {
	if newcfg == nil {
		return
	}

	gCutoverCfg.Store(*newcfg) // now coordinator can pick new cfg.
	evt := cal.NewCalEvent(EvtTypeCutover, "init_cfg_updated", cal.TransOK, "")
	evt.Completed()
	// now ensure the change-triggered action are done as well
	// update workerpool
	maxtype := int(wtypeRW)
	if GetConfig().ReadonlyPct > 0 {
		maxtype += 1
	}
	for shid := 0; shid < int(MaxDbInCutover); shid++ {
		for t := 0; t <= maxtype; t++ {
			wpool, initerr := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
			if initerr != nil {
				evtn := fmt.Sprint("init_wpool_err_", shid, "_", t)
				evt = cal.NewCalEvent(EvtTypeCutover, evtn, cal.TransOK, initerr.Error())
				evt.Completed()
				logger.GetLogger().Log(logger.Warning, "loadCutoverCfg() [shid, wtype] [", shid, ",", t, "]", initerr.Error())
			} else {
				if wpool != nil {
					tname := gTnsAlias
					if shid == int(ShIdTnsCutover) {
						tname = gTnsAliasCutover
					}
					wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbByTns[tname], (tname == newcfg.TnsByRole[Source]))
				} else {
					evtn := fmt.Sprint("init_wpool_nil_", shid, "_", t)
					evt = cal.NewCalEvent(EvtTypeCutover, evtn, cal.TransOK, "")
					evt.Completed()
					logger.GetLogger().Log(logger.Warning, "loadCutoverCfg() can't get workerpool [shid, type] [", shid, ",", t, "]")
				}
				wpool = nil
			}
		}
	}
	immediateStopReq(nil, newcfg)
	setCheckUserRoleFlag(newcfg)
	cfgwkrchange := GetConfig().NumWorkersChW()
	cfgwkrchange <- getPoolSizePolicy(newcfg.Phase, newcfg.TnsByRole[Source])
}

func validateRecord(r1 CutoverRecord, r2 CutoverRecord) error {
	if !(isValidDbRole(r1.tnsAliasRole.String, r2.tnsAliasRole.String)) {
		return fmt.Errorf("error cutover cfg invalid tns_alias_role [%s, %s]", r1.tnsAliasRole.String, r2.tnsAliasRole.String)
	}

	// can't have identical OCC_TNS_ALIAS
	if r1.occTnsAlias.String == r2.occTnsAlias.String {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_same_occ_tns_alias", cal.TransOK, "")
		evt.Completed()
		return fmt.Errorf("error cutover cfg can't have same two_task [%s, %s] [%s, %s]",
			r1.occTnsAlias.String, r2.dbUname.String, r1.occTnsAlias.String, r2.dbUname.String)
	}

	// both the returned rows have identical value of cutover_phase
	if r1.phase.String != r2.phase.String {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_inconsist_phase", cal.TransOK, "")
		evt.Completed()
		return fmt.Errorf("error: query result has inconsistent phase %s, %s", r1.phase.String, r2.phase.String)
	}

	// the value of cutover_phase in the two rows is not among ‘Enable’, ‘Pre’, ‘Cutover’, ‘Complete’ (case insensitive)
	ph := validatePhase(r1.phase.String)
	if ph == 0 {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_invalid_phase", cal.TransOK, "")
		evt.Completed()
		return fmt.Errorf("error: query result has invalid cutover_phase %s", r1.phase.String)

	}
	// can't have identical dbuname. we shouldn't need to allow this.
	if r1.dbUname.String == r2.dbUname.String {
		if ph == CutoverPhId || ph == FlexupPhId {
			evt := cal.NewCalEvent(EvtTypeCutover, "err_same_dbuname", cal.TransOK, "")
			evt.Completed()
			return fmt.Errorf("error:  can't have identical dbuname [%s:%s], [%s:%s] at cutover",
				r1.occTnsAlias.String, r1.dbUname.String, r2.occTnsAlias.String, r2.dbUname.String)
		}
	}

	if !(isValidRw(r1.rstatus.String, r1.wstatus.String)) {
		return fmt.Errorf("1 error: read or write status invalid")
	}
	if !(isValidRw(r2.rstatus.String, r2.wstatus.String)) {
		return fmt.Errorf("2 error: read or write status invalid")
	}
	return nil
}

func populateNewCfg(rcrds [2]CutoverRecord) (CutoverCfg, error) {
	var outcfg CutoverCfg
	outcfg.DbByTns = make(map[string]string, 3)
	outcfg.RWstatusByDb = make(map[string]int, 3)
	outcfg.TnsByRole = make(map[string]string, 3)
	outcfg.Phase = rcrds[0].phase.String
	active := 0
	for i := 0; i < 2; i++ {
		rec2task := rcrds[i].occTnsAlias.String
		recDbRole := rcrds[i].tnsAliasRole.String
		recDbUname := rcrds[i].dbUname.String
		recRstatus := rcrds[i].rstatus.String
		recWstatus := rcrds[i].wstatus.String
		outcfg.DbByTns[rec2task] = recDbUname
		outcfg.TnsByRole[recDbRole] = rec2task
		outcfg.RWstatusByDb[recDbUname] = 0
		if recRstatus[0] == 'Y' {
			outcfg.RWstatusByDb[recDbUname] |= ReadOk
		}
		if recWstatus[0] == 'Y' {
			outcfg.RWstatusByDb[recDbUname] |= WriteOk
		}

		logger.GetLogger().Log(logger.Debug, "rec", i, " newcfg RWStatusByDb[", recDbUname, "] = ", outcfg.RWstatusByDb[recDbUname])

		if outcfg.RWstatusByDb[recDbUname] > 0 {
			active++
			outcfg.ActiveTns = rec2task
			logger.GetLogger().Log(logger.Debug, "rec", i, "active db - two_task", outcfg.ActiveTns, ", RWStatusByDb =", outcfg.RWstatusByDb[outcfg.DbByTns[rec2task]])

			// based on two_task name, set active shard id
			if rec2task == gTnsAlias {
				outcfg.ActiveShardId = ShIdTns
			} else if rec2task == gTnsAliasCutover {
				outcfg.ActiveShardId = ShIdTnsCutover
			} else {
				// this should never happen w/ the defined sql
				evt := cal.NewCalEvent(EvtTypeCutover, "err_undefined_occ_tns_alias", cal.TransOK, rec2task)
				evt.Completed()
				logger.GetLogger().Log(logger.Warning, "error: occ2task not match defined two_task or two_task_cutover", rec2task)
				outcfg.ActiveTns = UnsetStr
				outcfg.ActiveShardId = ShIdUnset
			}
			if active >= 2 {
				evt := cal.NewCalEvent(EvtTypeCutover, "err_multi_active_db", cal.TransOK, "")
				evt.Completed()
				outcfg.ActiveTns = UnsetStr // just to be safe.
				return CutoverCfg{}, fmt.Errorf("error dual active db")
			}

		}
		logger.GetLogger().Log(logger.Debug, "done loading rec", i, "(two_task, phase, tns_alias_role, dbuname, wstatus, rstatus)(", rec2task, rcrds[i].phase, recDbRole, recDbUname, recWstatus, recRstatus, ")")
	}
	// set default active db for Enable, Pre, and Complete phases.
	if outcfg.Phase == EnablePhStr || outcfg.Phase == FlexupPhStr {
		srctns := outcfg.TnsByRole[Source]
		outcfg.ActiveTns = srctns
		if srctns == GetTnsName() {
			outcfg.ActiveShardId = ShIdTns
		} else if srctns == GetTnsCutoverName() {
			outcfg.ActiveShardId = ShIdTnsCutover
		} else {
			return CutoverCfg{}, fmt.Errorf("error invalid source tns")
		}
	}

	// handle no active db when phase is Cutover
	if active == 0 {
		evt := cal.NewCalEvent(EvtTypeCutover, "no_active_db", cal.TransOK, "")
		evt.Completed()
		outcfg.ActiveTns = UnsetStr
		outcfg.ActiveShardId = ShIdUnset
		evt = cal.NewCalEvent(EvtTypeCutover, "no_active_db", cal.TransOK, "")
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, "no active DB at Cutover phase")
	}

	return outcfg, nil
}

/*
db: sql.DB object, local: return local copy or update global copy.
This function sends the sql to fetch the config records and validate the requirement before proceed further.
 1. config sql above returns exact two rows of data
 7. Not allow Null from phase, dbUname, rstatus, wstatus, wisbroles.
 2. can't have identical value for OCC_TNS_ALIAS
 3. two rows must have consistent cutover_phase
 4. cutover_phase must be among ‘Enable’, ‘Pre’, ‘Cutover’, ‘Complete’ (case insensitive)
 5. two rows' dbuname must be different
 6. cutover_phase must be among ‘Enable’, ‘Pre’, ‘Cutover’, ‘Complete’ (case insensitive)
 7. read_status, write_status must be among ‘Y' or 'N’. (case insensitive)
    (Excluding cutover phase “Enable”), both the returned rows have 'Y' in read_status and/or write_status
*/
func loadCutoverCfg(db *sql.DB, localonly bool) (CutoverCfg, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer func() {
		cancel()
	}()
	conn, err := db.Conn(ctx)
	if err != nil {
		return CutoverCfg{}, fmt.Errorf("error: (conn) loading cutover cfg: %s", err.Error())
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, getCutoverSQL())
	if err != nil {
		return CutoverCfg{}, fmt.Errorf("error: (stmt) loading cutover cfg: %s", err.Error())
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return CutoverCfg{}, fmt.Errorf("error: (query) loading cutover cfg: %s", err.Error())
	}
	defer rows.Close()

	var records [2]CutoverRecord
	nrow := 0
	for rows.Next() {
		if nrow > 2 {
			return CutoverCfg{}, fmt.Errorf("error: fetched more than 2 rows from cfg table")
		}
		rec := &records[nrow]
		err = rows.Scan(&(rec.moduleName), &(rec.tnsAliasRole), &(rec.dbUname), &(rec.occTnsAlias), &(rec.phase), &(rec.wstatus), &(rec.rstatus), &(rec.wisbRoles))
		if err != nil {
			return CutoverCfg{}, err
		}
		nrow++
	}

	if nrow < 2 {
		return CutoverCfg{}, fmt.Errorf("error: expected 2 rows but get %d", nrow)
	}

	// Not allow Null from phase, dbUname, rstatus, wstatus, wisbroles.
	for i := 0; i < 2; i++ {

		if !records[i].tnsAliasRole.Valid {
			return CutoverCfg{}, fmt.Errorf("error cutover cfg has NULL string in tns_alias_role")
		}
		if !records[i].phase.Valid {
			return CutoverCfg{}, fmt.Errorf("error cutover cfg has NULL string in Phase")
		}
		if !records[i].dbUname.Valid {
			return CutoverCfg{}, fmt.Errorf("error cutover cfg has NULL string in Db_name")
		}
		if !records[i].rstatus.Valid {
			return CutoverCfg{}, fmt.Errorf("error cutover cfg has NULL string in read_status")
		}
		if !records[i].wstatus.Valid {
			return CutoverCfg{}, fmt.Errorf("error cutover cfg has NULL string in write_status")
		}
		if !records[i].wisbRoles.Valid {
			return CutoverCfg{}, fmt.Errorf("error cutover cfg has NULL string in wisb_roles")
		}
		records[i].tnsAliasRole.String = strings.TrimSpace(records[i].tnsAliasRole.String)
		records[i].phase.String = strings.TrimSpace(records[i].phase.String)
		records[i].dbUname.String = strings.TrimSpace(records[i].dbUname.String)
		records[i].occTnsAlias.String = strings.TrimSpace(records[i].occTnsAlias.String)
		records[i].rstatus.String = strings.TrimSpace(records[i].rstatus.String)
		records[i].wstatus.String = strings.TrimSpace(records[i].wstatus.String)
	}

	err = validateRecord(records[0], records[1])
	if err != nil {
		return CutoverCfg{}, err
	}

	// count how many active db while populating newcfg
	newcfg, err := populateNewCfg(records)
	if err != nil {
		return CutoverCfg{}, err
	}

	logger.GetLogger().Log(logger.Verbose, "Dump newcfg active db info (TwoTask, ShardId, Phase, rwstatus)=(",
		newcfg.ActiveTns, newcfg.ActiveShardId, newcfg.Phase, newcfg.RWstatusByDb[newcfg.DbByTns[newcfg.ActiveTns]], ")")
	if localonly {
		return newcfg, nil
	}
	/**
	The order of action is
	1. Update global copy (coordinator can load the latest instruction)
	2. Notify workerpools to abort in-progress txn according to new RW status
		// if fail, we won't retry this attempt but depends on kill long running sql detected by DB (3~5s)
	3. Update Workerpools with (phase and dbuname) and enforce conn integraty
		// if fail, cutover workerpools is left wrong dbuname. maybe we should notify workerpools even during no change.
		// Reset global copy, exit?
	4. Update workerpools to enable or disable check-set-user-role flag
		// if fail, workerpool has wrong flag, we should notify workerpools even during no change.
		// workerpool internally sends msg to workers if flag changes.
	5. Resize workerpool size if needed
		// if fail, the pool size can continue to be wrong. maybe we should notify workerpools even during no change.
	**/

	precfg := GetCutoverCfg()
	if precfg.Phase == "" {
		logger.GetLogger().Log(logger.Verbose, "INIT cutover cfg", newcfg)
		initUpdateGlobalCfg(&newcfg)

	} else {
		changed, changedAttr := CheckCfgChange(precfg, newcfg)
		if !changed {
			logger.GetLogger().Log(logger.Debug, "cutovercfg reload shows no change")
		}
		var curCfg CutoverCfg
		copyCutoverCfg(&curCfg, &precfg) // create a deep copy
		if changed {
			logger.GetLogger().Log(logger.Warning, "cutovercfg has new change", changed, changedAttr)
			evt := cal.NewCalEvent(EvtTypeCutover, "detect_cfg_change", cal.TransOK, "")
			evt.Completed()
			gCutoverCfg.Store(newcfg)
			immediateStopReq(&curCfg, &newcfg)
		}

		setCheckUserRoleFlag(&newcfg)
		maxtype := int(wtypeRW)
		if GetConfig().ReadonlyPct > 0 {
			maxtype += 1
		}

		var wpool *WorkerPool
		for shid := 0; shid < int(MaxDbInCutover); shid++ {
			for t := 0; t <= maxtype; t++ {
				wpool = nil
				wpool, err = GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
				if err != nil {
					evtn := fmt.Sprint("err_chg_info_get_wpool_", shid, "_", t)
					evt := cal.NewCalEvent(EvtTypeCutover, evtn, cal.TransOK, err.Error())
					evt.Completed()
					logger.GetLogger().Log(logger.Warning, "error cutovercfg failed to udpate workerpool ", shid, t)
				} else {
					// workerpool tracks phase, dbuname and enforce integrity at Pre, Cutover
					if wpool != nil {
						logger.GetLogger().Log(logger.Verbose, "loadCutoverCfg() [shid, wtype] [", shid, ",", t, "]")
						tname := gTnsAlias
						if shid == int(ShIdTnsCutover) {
							tname = gTnsAliasCutover
						}
						wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbByTns[tname], (tname == newcfg.TnsByRole[Source]))
					} else {
						evtn := fmt.Sprint("err_chg_info_wpool_", shid, "_", t)
						evt := cal.NewCalEvent(EvtTypeCutover, evtn, cal.TransOK, "can't get workerpool")
						evt.Completed()
						logger.GetLogger().Log(logger.Warning, "can't get workerpool [shid, type] [", shid, ",", t, "]")
					}
				}
			}
		}

		cfgwkrchange := GetConfig().NumWorkersChW()
		cfgwkrchange <- getPoolSizePolicy(newcfg.Phase, newcfg.TnsByRole[Source])
		logger.GetLogger().Log(logger.Debug, "cutovercfg change is processed and updated.")
	}

	return newcfg, nil
}

/*
	Return true if changed, false if the same. return changed attritubes value as:

0x0000  0 identical
0x0001  1 phase
0x0002  2 2taskShard's dbuname
0x0004  4 2taskCutoverShard's dbuname
0x0008  8 2taskShard's RW
0x0010 16 2taskCutoverShard's RW
0x0020 32 active_two_task
0x0040 64 tns_alias_role
*/
func CheckCfgChange(curcfg CutoverCfg, nextcfg CutoverCfg) (bool, int) {
	changed := false
	whatchanged := 0

	if curcfg.ActiveTns != nextcfg.ActiveTns {
		changed = true
		info := fmt.Sprint(curcfg.ActiveTns, "_to_", nextcfg.ActiveTns)
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_act_tns_diff", cal.TransOK, info)
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, "cfg ActiveTns is different", info)
		whatchanged |= 0x0020
	}

	if curcfg.Phase != nextcfg.Phase {
		changed = true
		info := fmt.Sprint(curcfg.Phase, "_to_", nextcfg.Phase)
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_phase_diff", cal.TransOK, info)
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, "cfg cutover phase is different", info)
		whatchanged |= 0x0001
	}

	if curcfg.DbByTns[gTnsAlias] != nextcfg.DbByTns[gTnsAlias] {
		changed = true
		info := fmt.Sprint(gTnsAlias, "_", curcfg.DbByTns[gTnsAlias], "_to_", nextcfg.DbByTns[gTnsAlias])
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_tns_db_diff", cal.TransOK, info)
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, gTnsAlias, "TnsAlias is different:", info)
		whatchanged |= 0x0002
	}

	if curcfg.DbByTns[gTnsAliasCutover] != nextcfg.DbByTns[gTnsAliasCutover] {
		changed = true
		info := fmt.Sprint(gTnsAliasCutover, "_", curcfg.DbByTns[gTnsAliasCutover], "_to_", nextcfg.DbByTns[gTnsAliasCutover])
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_tnscutover_db_diff", cal.TransOK, info)
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, gTnsAliasCutover, "TnsAliasCutover is different:", info)
		whatchanged |= 0x0004
	}

	dbun := curcfg.DbByTns[gTnsAlias]
	if curcfg.RWstatusByDb[dbun] != nextcfg.RWstatusByDb[dbun] {
		changed = true
		info := fmt.Sprint(dbun, "_", curcfg.RWstatusByDb[dbun], "_to_", nextcfg.RWstatusByDb[dbun])
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_tns_rw_status_diff", cal.TransOK, info)
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, dbun, "tnsalias rw status is different:", info)
		whatchanged |= 0x0008
	}

	dbun = curcfg.DbByTns[gTnsAliasCutover]
	if curcfg.RWstatusByDb[dbun] != nextcfg.RWstatusByDb[dbun] {
		changed = true
		info := fmt.Sprint(dbun, "_", curcfg.RWstatusByDb[dbun], "_to_", nextcfg.RWstatusByDb[dbun])
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_tnscutover_rw_status_diff", cal.TransOK, info)
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, dbun, "tnsaliascutover rw status is different:", info)
		whatchanged |= 0x0010
	}

	// Tns key and role definition are fixed so if tns for source changes, it means tns for target also changes.
	if curcfg.TnsByRole[Source] != nextcfg.TnsByRole[Source] {
		changed = true
		info := fmt.Sprint(curcfg.TnsByRole[Source], "_to_", nextcfg.TnsByRole[Source])
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_src_role_tns_diff", cal.TransOK, info)
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, gTnsAlias, "dbrole change is different:", info)
		whatchanged |= 0x00040

	}
	return changed, whatchanged
}

/*
when call workerpool of the shard that prev read_status or write _status or both, are changed from allowed to not allowed.
*/
func immediateStopReq(curcfg *CutoverCfg, nextcfg *CutoverCfg) {
	// Enter or continue in cutover phase: per rw status config, stop where it should
	// Exit cutover phase: take no action
	if nextcfg == nil {
		return
	}
	if nextcfg.Phase != CutoverPhStr { // we don't stop req for exiting Cutover
		return
	}

	if curcfg == nil { // at server init
		curcfg = nextcfg
	}

	curActDb := curcfg.DbByTns[curcfg.ActiveTns]
	curActTns := curcfg.ActiveTns
	nextAct2task := nextcfg.ActiveTns
	stopR := false
	stopW := false
	if nextcfg.Phase == CutoverPhStr {
		// remains in cutover phase
		if curActTns == nextAct2task {
			// active two_task remains the same
			if curActTns == UnsetStr { // no active db
				return
			}
			curRw := curcfg.RWstatusByDb[curActDb]
			nextRw := nextcfg.RWstatusByDb[curActDb]
			if curRw == nextRw {
				return
			}
			stopR = ((curRw & ReadOk) == ReadOk) && ((nextRw & ReadOk) == 0)
			stopW = ((curRw & WriteOk) == WriteOk) && ((nextRw & WriteOk) == 0)
			if !(stopR || stopW) { // nothing to stop
				return
			}
			stopRwCalName := fmt.Sprint("same_act_R-", !stopR, "_W-", !stopW)
			// sent the status to current active db.
			shid := int(curcfg.ActiveShardId)
			maxtype := int(wtypeRW)
			if GetConfig().ReadonlyPct > 0 {
				maxtype += 1
			}
			for t := 0; t <= maxtype; t++ {
				logger.GetLogger().Log(logger.Warning, "same active two_task, stop in-progress request. stopR =", stopR, ", stopW =", stopW)
				wpool, err := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
				if err != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "err_wpool_stopRW", cal.TransOK, stopRwCalName)
					evt.Completed()
					logger.GetLogger().Log(logger.Alert, "error: can't get workerpool", t, "error:", err.Error())
				} else {
					if wpool != nil {
						evt := cal.NewCalEvent(EvtTypeCutover, stopRwCalName, cal.TransOK, "")
						evt.Completed()
						wpool.StopWorker(stopR, stopW)
					} else {
						logger.GetLogger().Log(logger.Alert, "error: can't get workerpool to stop r/w [shid, type] [", shid, ",", t, "]")
					}
					wpool = nil
				}
			}
		} else {
			// cur active2task and next active2task are different
			//  * stop write on current active2task
			//  * no action needed for next active2task
			if curActTns == UnsetStr { // from none to one active
				return
			}
			stopR = true
			stopW = true
			stopRwCalName := fmt.Sprint("act_chg_stop_RW_", curActTns)
			shid := int(curcfg.ActiveShardId)
			maxtype := int(wtypeRW)
			if GetConfig().ReadonlyPct > 0 {
				maxtype += 1
			}
			for t := 0; t <= maxtype; t++ {
				logger.GetLogger().Log(logger.Warning, "active two_task changes, stop in-progress request. stopR =", stopR, ", stopW =", stopW)
				wpool, err := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
				if err != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "err_wpool_stopRW_diff_tns", cal.TransOK, stopRwCalName)
					evt.Completed()
					logger.GetLogger().Log(logger.Alert, "err_wpool_stopRW_diff_tns", t, "error:", err.Error())
				} else {
					if wpool != nil {
						evt := cal.NewCalEvent(EvtTypeCutover, "notify_wp_rw_status", cal.TransOK, "")
						evt.Completed()
						wpool.StopWorker(stopR, stopW)
					} else {
						logger.GetLogger().Log(logger.Alert, "workerpool nil. [shid, type] [", shid, ",", t, "]")
					}
					wpool = nil
				}
			}
		}
	}
}

func cutoverOpenDb(shToUse ShardByTwoTask) (*sql.DB, error) {
	logger.GetLogger().Log(logger.Debug, "cutoverOpenDb to shard:", shToUse)

	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", int(shToUse)))
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	return db, nil
}

// takes string input and return cooresponding non-zero phase ID. return 0 if input phase is defined
func validatePhase(phase string) int {
	switch phase {
	case EnablePhStr:
		return EnablePhId
	case FlexupPhStr:
		return FlexupPhId
	case CutoverPhStr:
		return CutoverPhId
	default:
		logger.GetLogger().Log(logger.Warning, "config phase is invalid", phase)
		return 0
	}

}

// return value indicates which tns pool to minimize
// less or equal to 0, ignore
// 1 -> minimize tns_cutover pool size
// 2 -> minimize tns pool size
// 3 -> tns and tns_cutover both full
func getPoolSizePolicy(phase string, srcTns string) int {
	phid := validatePhase(phase)
	if phid <= 0 {
		return phid
	}

	if phid == EnablePhId {
		//The only phase asymmetric pool size
		if srcTns == GetTnsName() {
			return 1
		}
		if srcTns == GetTnsCutoverName() {
			return 2
		}
	} else {
		// pre and cutover
		if srcTns == GetTnsName() || srcTns == GetTnsCutoverName() {
			return 3
		}
	}
	return 0
}

func isValidDbRole(role1 string, role2 string) bool {
	// can't be the same
	if role1 == role2 {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_same_tnsRole", cal.TransOK, "")
		evt.Completed()
		return false
	}
	// must be either 'SRC' or 'TGT'
	if !(role1 == Source || role1 == Target) || !(role2 == Source || role2 == Target) {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_invalid_tnsRole", cal.TransOK, "")
		evt.Completed()
		return false
	}
	return true
}

// rec1 and rec2 are string representing write/read_status
// anything besides 'Y' or 'N' of first letter will return false
func isValidRw(rec1 string, rec2 string) bool {
	isValid := false
	if (rec1[0] == 'Y' || rec1[0] == 'N') && (rec2[0] == 'Y' || rec2[0] == 'N') {
		isValid = true
	}
	return isValid
}

// Precedence: 1st -> 2nd
// TWO_TASK_0 -> TWO_TASK
// TWO_TASK_CUTOVER_0 -> TWO_TASK_CUTOVER
// TWO_TASK_READ_0 -> TWO_TASK_READ
// TWO_TASK_READ_CUTOVER_0 -> TWO_TASK_READ_CUTOVER
func setPermTwoTaskName() error {
	gTnsAlias = strings.ToUpper(os.Getenv("TWO_TASK_0"))
	if gTnsAlias == "" {
		gTnsAlias = strings.ToUpper(os.Getenv("TWO_TASK"))
	}
	gTnsAliasCutover = strings.ToUpper(os.Getenv("TWO_TASK_CUTOVER_0"))
	if gTnsAliasCutover == "" {
		gTnsAliasCutover = strings.ToUpper(os.Getenv("TWO_TASK_CUTOVER"))
	}

	if gTnsAlias == "" || gTnsAliasCutover == "" {
		return fmt.Errorf("error incomplete cutover env setup [%s] [%s]", gTnsAlias, gTnsAliasCutover)
	}

	if GetConfig().ReadonlyPct > 0 {
		gTnsAliasR = strings.ToUpper(os.Getenv("TWO_TASK_READ_0"))
		if gTnsAliasR == "" {
			gTnsAliasR = strings.ToUpper(os.Getenv("TWO_TASK_READ"))
		}
		gTnsAliasCutoverR = strings.ToUpper(os.Getenv("TWO_TASK_READ_CUTOVER_0"))
		if gTnsAliasCutoverR == "" {
			gTnsAliasCutoverR = strings.ToUpper(os.Getenv("TWO_TASK_READ_CUTOVER"))
		}
		if gTnsAliasCutoverR == "" || gTnsAliasCutover == "" {
			// can't proceed
			return fmt.Errorf("error incomplete read-only cutover env setup [%s] [%s]", gTnsAliasR, gTnsAliasCutoverR)
		}
	}
	return nil
}


func setCheckUserRoleFlag(nextcfg *CutoverCfg) {
	if nextcfg == nil {
		logger.GetLogger().Log(logger.Alert, "cutover setUserRole cannot determine next cutovercfg")
		return
	}

	var execSetUserRole uint = 0 // 0 disable, >0 enable
	if nextcfg.Phase == CutoverPhStr {
		execSetUserRole = 1
	}
	maxtype := int(wtypeRW)
	if GetConfig().ReadonlyPct > 0 {
		maxtype += 1
	}
	for shid := 0; shid < int(MaxDbInCutover); shid++ {
		for t := 0; t <= maxtype; t++ {
			wpool, err := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
			if err != nil {
				evtn := fmt.Sprint("err_wpool_user_flag_", shid, "_", t)
				evt := cal.NewCalEvent(EvtTypeCutover, evtn, cal.TransOK, err.Error())
				evt.Completed()
				logger.GetLogger().Log(logger.Warning, "error cutover set_user_role_flag[", shid, ",", t, "]", err.Error())
			} else {
				if wpool != nil {
					logger.GetLogger().Log(logger.Warning, "cutover set_user_role_flag [", shid, ",", t, "] to ", execSetUserRole)
					wpool.CheckSetUserRole(execSetUserRole)
				} else {
					evtn := fmt.Sprint("err_wpool_user_flag", shid, "_", t)
					evt := cal.NewCalEvent(EvtTypeCutover, evtn, cal.TransOK, "wpool nil")
					evt.Completed()
					logger.GetLogger().Log(logger.Alert, "error cutover set_user_role_flag nil wpool [", shid, ",", t, "]")
				}
			}
		}
	}
}
