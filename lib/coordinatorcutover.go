package lib

import (
	"errors"
	"strconv"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

// Active shard means the shard either take user requests.
// ShId = ShIdUnset and DbUname = UnsetStr if no active shard. 
type ActiveDbInfo struct {
	SrcTns   string         // source Tns key
	SrcShId  ShardByTwoTask // source shard id
	ShId     ShardByTwoTask // active pool shard id
	Phase    string         // current cutover phase
	DbUname  string         // Active DB_UNAME
	RwStatus int            // dbuname --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
}

func (crd *Coordinator) copyActCOInfo(destInfo *ActiveDbInfo, srcInfo ActiveDbInfo) {
	if destInfo == nil {
		destInfo = &ActiveDbInfo{}
	}
	destInfo.SrcTns = srcInfo.SrcTns
	destInfo.SrcShId = srcInfo.SrcShId
	destInfo.ShId = srcInfo.ShId
	destInfo.Phase = srcInfo.Phase
	destInfo.RwStatus = srcInfo.RwStatus
	destInfo.DbUname = srcInfo.DbUname
}

// Compare existing ActiveCOInfo with new cfg.
// 0 identical
// 1 if twotask changes (shard id changes)
// 2 if dbuname changes
// 4 if phase changes (may force shard id )
// 8 if RWStatus changes
// 16 if SrcTns changes
func compActiveInfo(cur *ActiveDbInfo, newcfg *ActiveDbInfo) int {
	if cur == nil || newcfg == nil {
		return -1
	}
	flag := 0
	if cur.ShId != newcfg.ShId {
		flag |= 0x0001
	}
	if cur.DbUname != newcfg.DbUname {
		flag |= 0x0002
	}
	if cur.Phase != newcfg.Phase {
		flag |= 0x0004
	}
	if cur.RwStatus != newcfg.RwStatus {
		flag |= 0x0008
	}
	if cur.SrcTns != newcfg.SrcTns {
		flag |= 0x0010
	}
	return flag
}

// Build the ActiveCOInfo from CutoverCfg.
func cvtActiveInfo(cocfg *CutoverCfg) *ActiveDbInfo {
	if cocfg == nil {
		return nil
	}
	var newActInfo ActiveDbInfo
	newActInfo.SrcTns = cocfg.TnsByRole[Source]
	newActInfo.SrcShId = ShIdTns
	if newActInfo.SrcTns == gTnsAliasCutover {
		newActInfo.SrcShId = ShIdTnsCutover
	}

	if cocfg.Phase == EnablePhStr || cocfg.Phase == FlexupPhStr {
		newActInfo.ShId = newActInfo.SrcShId
		newActInfo.DbUname = cocfg.DbByTns[gTnsAlias]
		newActInfo.Phase = cocfg.Phase
		newActInfo.RwStatus = (ReadOk | WriteOk)
	} else {
		//cutover
		if cocfg.ActiveTns == UnsetStr {
			evt := cal.NewCalEvent(EvtTypeCutover, "cutover_no_active_db", cal.TransOK, "")
			evt.Completed()
			logger.GetLogger().Log(logger.Warning, "No active DB in cutover phase", cocfg)
			newActInfo.ShId = ShIdUnset
			newActInfo.DbUname = UnsetStr
			newActInfo.Phase = cocfg.Phase
		} else {
			actDb := cocfg.DbByTns[cocfg.ActiveTns]
			newActInfo.DbUname = actDb
			newActInfo.Phase = cocfg.Phase
			newActInfo.ShId = cocfg.ActiveShardId
			newActInfo.RwStatus = cocfg.RWstatusByDb[actDb]
		}
	}
	logger.GetLogger().Log(logger.Debug, "ActiveDBInfo (ActShId, SrcShId, dbUname, phase, rwstatus)=(", newActInfo.ShId, newActInfo.SrcShId, newActInfo.DbUname, newActInfo.Phase, newActInfo.RwStatus, ")")
	return &newActInfo
}

/*
PreprocessCutover returns bool: true -> hang up client connection. error: if there is an error in process.
The function loads the latest cfg and detect which pool shard it should go. Disconnect the client if needed.
hang up conditions
1. active tns alias has changed from last tracked active info in this coordinator
2. active tns alias is unchanged but RWstatus disabled from enabled.
3. if no active config can be constructed, return hangup true
*/
func (crd *Coordinator) PreprocessCutover(requests []*netstring.Netstring) (bool, error) {

	tmpcfg := GetCutoverCfg()
	if tmpcfg.Phase == "" {
		if !crd.isInternal {
			evt := cal.NewCalEvent(EvtTypeCutover, "preproc_cfg_nil_startup", cal.TransOK, "")
			evt.Completed()
			logger.GetLogger().Log(logger.Info, crd.id, "cutovercfg is nil, likely at server start up")
			return true, nil
		}
		evt := cal.NewCalEvent(EvtTypeCutover, "preproc_internal_startup", cal.TransOK, "")
		evt.Completed()
		return false, nil
	}

	interrupt := false // if txn should be disrupted
	newActInfo := cvtActiveInfo(&tmpcfg)
	if newActInfo == nil {
		evt := cal.NewCalEvent(EvtTypeCutover, "preproc_empty_newactive", cal.TransOK, "")
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, "something is wrong. no new activeInfo in PreprocessCutover")
		if crd.curActDb != nil {
			logger.GetLogger().Log(logger.Debug, crd.id, "new cfg nil, existing active [ShId, , SrcShId, dbUname, phase, rwstatus]=[",
				crd.curActDb.ShId, crd.curActDb.SrcShId, crd.curActDb.DbUname, crd.curActDb.Phase, crd.curActDb.RwStatus, "]")
		}
		return interrupt, nil
	}

	if crd.curActDb == nil {
		logger.GetLogger().Log(logger.Verbose, crd.id, "PreprocessCutover crd.curActInfo is nil, expected when coordinator is just created")
		crd.curActDb = &ActiveDbInfo{}
		crd.copyActCOInfo(crd.curActDb, *newActInfo) // now coordinator has updated with latest info
		logger.GetLogger().Log(logger.Debug, crd.id, "init crd.curActDb [ShId, SrcShId, dbUname, phase, rwstatus]=[",
			crd.curActDb.ShId, crd.curActDb.SrcShId, crd.curActDb.DbUname, crd.curActDb.Phase, crd.curActDb.RwStatus, "]")
	}
	diff := compActiveInfo(crd.curActDb, newActInfo)

	if diff < 0 {
		logger.GetLogger().Log(logger.Alert, crd.id, "PreprocessCutover unexpected nil at comparing ActiveInfo")
		return interrupt, nil
	}

	if diff == 0 {
		return interrupt, nil // same cutover config
	}

	logger.GetLogger().Log(logger.Verbose, "crd.curActInfo and newActInfo is different", diff)
	var err error
	if crd.inTransaction {
		if (diff & 0x0001) == 0x0001 { // active tns changes.
			if !crd.isInternal {
				if diff&0x0004 == 0x0004 { // phase changes
					if (crd.curActDb.Phase == CutoverPhStr) && (crd.curActDb.ShId != newActInfo.SrcShId) {
						// cutover -> flexup or enable phase
						// compare cur active tns and SrcTns, if mismatch, error
						logger.GetLogger().Log(logger.Warning, crd.id, "enter new phase enable/flexup, cur txn is not using src pool")
						evt := cal.NewCalEvent(EvtTypeCutover, "crd_act_tns_chg_exit_cutover", cal.TransOK, "")
						evt.Completed()
						interrupt = true
						err = errors.New("crd_act_tns_chg_exit_cutover")

					} else if newActInfo.Phase == CutoverPhStr && (crd.curActDb.SrcShId != newActInfo.ShId) {
						logger.GetLogger().Log(logger.Warning, crd.id, "enter new phase cutover, cur txn is not using active tns")
						evt := cal.NewCalEvent(EvtTypeCutover, "crd_act_tns_chg_enter_cutover", cal.TransOK, "")
						evt.Completed()
						interrupt = true
						err = errors.New("crd_act_tns_chg_enter_cutover")
					} else {
						// enable -> flex up or flex up -> enable
						// compare cur and new SrcTns, if mismatch, error
						if diff&0x0010 == 0x0010 {
							logger.GetLogger().Log(logger.Warning, crd.id, "cur txn is not using active tns")
							evt := cal.NewCalEvent(EvtTypeCutover, "crd_act_tns_chg_enter_cutover", cal.TransOK, "")
							evt.Completed()
							interrupt = true
							err = errors.New("crd_act_tns_chg_enter_cutover")
						}
					}
				} else {
					// phase is the same. we only need to see check if current phase is cutover
					if crd.curActDb.Phase == CutoverPhStr {
						logger.GetLogger().Log(logger.Warning, crd.id, "cutover phase active tns changes")
						evt := cal.NewCalEvent(EvtTypeCutover, "crd_act_tns_chg_at_cutover", cal.TransOK, "")
						evt.Completed()
						interrupt = true
						err = errors.New("crd_act_tns_chg_at_cutover")
					} else {
						// phase remains in enable or flex up, check if SrcTns changed
						if diff&0x0010 == 0x0010 {
							logger.GetLogger().Log(logger.Warning, crd.id, "enable/flex up phase src tns changes")
							evt := cal.NewCalEvent(EvtTypeCutover, "crd_src_tns_chg_not_cutover", cal.TransOK, "")
							evt.Completed()
							interrupt = true
							err = errors.New("crd_src_tns_chg_not_cutover.")
						}
					}
				}
			} else {
				if diff&0x0010 == 0x0010 {
					logger.GetLogger().Log(logger.Warning, crd.id, "crd internal src tns changes")
					evt := cal.NewCalEvent(EvtTypeCutover, "crd_internal_src_tns_chg", cal.TransOK, "")
					evt.Completed()
				}
			}
		}
		if (diff & 0x0002) == 0x0002 {
			evt := cal.NewCalEvent(EvtTypeCutover, "crd_see_dbuname_change", cal.TransOK, "")
			evt.Completed()
			logger.GetLogger().Log(logger.Info, crd.id, "logging only. crd cutover preprocess active dbuname change")
		}

		if (diff & 0x0008) == 0x0008 {
			// if this has a stop to read or write, we will take action by stopping the intransaction
			// rw status, 1 R, 2 W, 3 RW, 0 NRNW
			if newActInfo.Phase == CutoverPhStr {
				if crd.curActDb.RwStatus > newActInfo.RwStatus { // either W or R or both RW are newly disabled.
					if newActInfo.RwStatus == 0 {
						evt := cal.NewCalEvent(EvtTypeCutover, "crd_stop_in_txn_rw", cal.TransOK, "")
						evt.Completed()
						interrupt = true //we will check this later
						err = errors.New("cutover stop in-txn")
					}
					if newActInfo.RwStatus&0x0001 == 0 { // read is disabled now
						// stop READ
						if crd.isRead {
							evt := cal.NewCalEvent(EvtTypeCutover, "crd_stop_in_txn_r", cal.TransOK, "")
							evt.Completed()
							interrupt = true // we will check this later
							err = errors.New("cutover stop in-txn read")
						}
					}
					if newActInfo.RwStatus&0x0002 == 0 { // write is disabled now
						// stop WRTIE
						if !crd.isRead {
							evt := cal.NewCalEvent(EvtTypeCutover, "crd_stop_in_txn_w", cal.TransOK, "")
							evt.Completed()
							interrupt = true // we will check this later
							err = errors.New("cutover top in-txn write")
						}
					}
				}
			}
		}
		crd.copyActCOInfo(crd.curActDb, *newActInfo) // now coordinator has updated with latest info
		crd.shard.shardID = int(crd.curActDb.ShId)   // we will need shardID in dispatchRequest(). hm..
		return interrupt, err
	} else {
		// worker is not in transactions
		// we will just load the new cfg and update the crd flags as needed.
		crd.copyActCOInfo(crd.curActDb, *newActInfo)
		crd.shard.shardID = int(crd.curActDb.ShId)
		return false, nil // allow to proceed
	}
}

func (crd *Coordinator) ProceedReadInCutover() error {
	if (crd.curActDb.Phase == CutoverPhStr) && ((crd.curActDb.RwStatus & ReadOk) != ReadOk) {
		evt := cal.NewCalEvent(EvtTypeCutover, "no_read_to_act_db", cal.TransOK, "")
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, crd.id, "OCC-500: active db cutover no read allowed")
		return ErrCutoverReadNotAllowed
	}
	return nil
}

func (crd *Coordinator) ProceedWriteInCutover() error {
	if (crd.curActDb.Phase == CutoverPhStr) && ((crd.curActDb.RwStatus & WriteOk) != WriteOk) {
		evt := cal.NewCalEvent(EvtTypeCutover, "no_write_to_act_db", cal.TransOK, "")
		evt.Completed()
		logger.GetLogger().Log(logger.Warning, crd.id, "OCC-501: active db cutover no write allowed")
		return ErrCutoverWriteNotAllowed
	}
	return nil
}

func (crd *Coordinator) getSrcShardByCutoverCfg() ShardByTwoTask {
	if crd.curActDb == nil {
		logger.GetLogger().Log(logger.Warning, crd.id, "unknown source, ignore during server init")
		// we don't know yet
		return ShIdUnset
	}
	logger.GetLogger().Log(logger.Debug, crd.id, "get ActiveDb source tns", crd.curActDb.SrcTns)
	srcShId := ShIdTns
	if crd.curActDb.SrcTns == GetTnsCutoverName() {
		srcShId = ShIdTnsCutover
	}
	logger.GetLogger().Log(logger.Debug, crd.id, "ActiveDb source tns", srcShId)
	// reset the internal
	crd.intSessionShId = ShIdUnset
	return srcShId
}

// getActiveShId return the active shard id based on phase and read/write status. Not for internal sqls.
func (crd *Coordinator) getActiveShId() (ShardByTwoTask, error) {
	shardToUse := ShIdUnset
	if crd.curActDb.Phase == CutoverPhStr {
		logger.GetLogger().Log(logger.Verbose, crd.id, "cutover phase crd.isRead [", crd.isRead, "] crd.curActInfo.Arwstatus [", crd.curActDb.RwStatus, "]")
		if crd.isRead {
			if (crd.curActDb.RwStatus & ReadOk) != ReadOk {
				logger.GetLogger().Log(logger.Info, crd.id, "OCC-500: active db cutover no read allowed")
				return shardToUse, ErrCutoverReadNotAllowed
			}
		} else {
			if (crd.curActDb.RwStatus & WriteOk) != WriteOk {
				logger.GetLogger().Log(logger.Info, crd.id, "OCC-501: active db cutover no write allowed")
				return shardToUse, ErrCutoverWriteNotAllowed
			}
		}

		shardToUse = crd.curActDb.ShId
		logger.GetLogger().Log(logger.Verbose, crd.id, "cutover phase, crd dispatch to", int(shardToUse), "workers")
	} else if crd.curActDb.Phase == EnablePhStr || crd.curActDb.Phase == FlexupPhStr {
		shardToUse = crd.curActDb.SrcShId
		logger.GetLogger().Log(logger.Verbose, crd.id, "enable/flexupi phase, crd dispatch to ", int(shardToUse), "workers")
		if !isValidShToCutover(shardToUse) {
			// we can't default sql routing by unknown actual config
			shardToUse = ShIdUnset
			return shardToUse, ErrSrcUnknown
		}
	} else {
		logger.GetLogger().Log(logger.Verbose, crd.id, "dispatchRequest error invalid cutover phase")
		return shardToUse, errors.New("invalid cutover phase")
	}
	return shardToUse, nil
}

// only for internal write queries. When read cfg always use tns alias shard, write uses tns alias shard and cutover shard
func (crd *Coordinator) processSetInternalShID(val []byte) error {
	if !GetConfig().EnableCutover { // no need to pass
		crd.intSessionShId = ShIdUnset
		return nil
	}
	if !crd.isInternal { // not allow external connections
		return ErrNotInternal
	}

	sh, err := strconv.ParseInt(string(val), 10, 32)
	logger.GetLogger().Log(logger.Debug, crd.id, "processSetInternalShID", sh)
	if err != nil {
		return nil
	}
	// cutover enabled. we expect sh to be 0 (tns alias) or 1 (tns alias_cutover)
	if !(sh == 0 || sh == 1) {
		return ErrBadShardID
	}

	crd.intSessionShId = ShardByTwoTask(sh)
	if crd.inTransaction && (crd.worker != nil) {
		// crd.worker.shardID is always used so check if we need switch. (unlikely if we don't reuse connection)
		logger.GetLogger().Log(logger.Debug, crd.id, "processSetInternalShID", crd.intSessionShId, "crd.worker.shardID", crd.worker.shardID)
		if int(crd.intSessionShId) != crd.worker.shardID {
			evt := cal.NewCalEvent(EvtTypeCutover, "internal query change pool", cal.TransOK, "")
			evt.AddDataInt("cur_shard_id", int64(crd.worker.shardID))
			evt.AddDataStr("requested_shard_id", string(val))
			evt.Completed()
			return ErrChangeShardIDInTxn
		}
	}
	return nil
}

func isValidShToCutover(sh ShardByTwoTask) bool {
	return (sh == 0 || sh == 1)
}
