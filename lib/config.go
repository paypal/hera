// Copyright 2019 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lib

import (
	"errors"
	"fmt"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/config"
	"github.com/paypal/hera/utility/logger"
	otelconfig "github.com/paypal/hera/utility/logger/otel/config"

	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

const (
	mux_config_cal_name           = "OCC_CONFIG"
	oracle_worker_config_cal_name = "OCC_ORACLE_WORKER_CONFIG"
)

// The Config contains all the static configuration
type Config struct {
	CertChainFile   string
	KeyFile         string // leave blank for no SSL
	Port            int
	ChildExecutable string
	//
	// worker sizing
	//
	NumStdbyDbs        int
	InitialMaxChildren int
	ReadonlyPct        int
	TafChildrenPct 	   int
	//
	// backlog
	//
	BacklogPct                  int
	BacklogTimeoutMsec          int
	BacklogTimeoutUnit          int64
	ShortBacklogTimeoutMsec     int
	SoftEvictionEffectiveTimeMs int
	SoftEvictionProbability     int
	BindEvictionTargetConnPct   int
	BindEvictionThresholdPct    int
	BindEvictionDecrPerSec      float64
	BindEvictionNames           string
	BindEvictionMaxThrottle     int
	SkipEvictRegex              string
	EvictRegex                  string
	//
	//
	//
	BouncerEnabled bool
	//
	// second
	//
	BouncerStartupDelay int
	//
	// millisecond
	//
	BouncerPollInterval int
	//
	// config_reload_time_ms(30 * 1000)
	//
	ConfigReloadTimeMs int
	//
	//
	ConfigLoggingReloadTimeHours int
	// custom_auth_timeout(1000)
	CustomAuthTimeoutMs int
	// time_skew_threshold_warn(2)
	TimeSkewThresholdWarnSec int
	// time_skew_threshold_error(15)
	TimeSkewThresholdErrorSec int
	// max_stranded_time_interval(2000)
	StrandedWorkerTimeoutMs         int
	HighLoadStrandedWorkerTimeoutMs int
	HighLoadSkipInitiateRecoverPct  int
	HighLoadPct                     int
	InitLimitPct                    int

	// the worker scheduler policy
	LifoScheduler bool

	//
	// @TODO need a function for cdb boolean
	//
	DatabaseType              dbtype
	EnableSharding            bool
	UseShardMap               bool
	NumOfShards               int
	ShardKeyName              string
	MaxScuttleBuckets         int
	ScuttleColName            string
	ShardingAlgoHash          bool
	ShardKeyValueTypeIsString bool

	EnableWhitelistTest       bool
	NumWhitelistChildren      int
	ShardingPostfix           string
	ShardingCfgReloadInterval int

	HostnamePrefix       map[string]string
	ShardingCrossKeysErr bool

	CfgFromTns                  bool
	CfgFromTnsOverrideNumShards int // -1 no-override
	CfgFromTnsOverrideTaf       int // -1 no-override, 0 override-false, 1 override-true
	CfgFromTnsOverrideRWSplit   int // -1 no-override, readChildPct

	//
	// statelog printing interval (in sec)
	//
	StateLogInterval int

	// flag to enable CLIENT_INFO to worker
	EnableCmdClientInfoToWorker bool

	// if TAF is enabled
	EnableTAF bool
	// Timeout for a query to run on the primary, before fallback to secondary
	TAFTimeoutMs uint32

	// for adaptive timeouts, how long a window to try to keep
	TAFBinDuration       int
	TAFAllowSlowEveryX   int
	TAFNormallySlowCount int

	// for testing, enabling profile
	EnableProfile     bool
	ProfileHTTPPort   string
	ProfileTelnetPort string
	// to use OpenSSL (for testing) or crypto/tls
	UseOpenSSL bool

	ErrorCodePrefix       string
	StateLogPrefix        string
	ManagementTablePrefix string
	// RAC maint reload config interval
	RacMaintReloadInterval int
	// worker restarts are spread over this window
	RacRestartWindow int

	// worker lifespan check interval
	lifeSpanCheckInterval int

	MuxPidFile string

	// when numWorkers changes, it will write to this channel, for worker manager to update
	numWorkersCh chan int

	EnableConnLimitCheck         bool
	EnableQueryBindBlocker       bool
	QueryBindBlockerMinSqlPrefix int

	// taf testing
	TestingEnableDMLTaf bool

	//
	// enable background goroutine to recover worker not returned by coordinator
	//
	EnableDanglingWorkerRecovery bool

	GoStatsInterval int
	RandomStartMs   int

	// The max number of database connections to be established per second
	MaxDbConnectsPerSec int

	// Max desired percentage of healthy workers for the worker pool
	MaxDesiredHealthyWorkerPct int

	// Oracle Worker Configs
	EnableCache            bool
	EnableHeartBeat        bool
	EnableQueryReplaceNL   bool
	EnableBindHashLogging  bool
	EnableSessionVariables bool
	UseNonBlocking         bool
}

// The OpsConfig contains the configuration that can be modified during run time
type OpsConfig struct {
	logLevel               int
	numWorkers             uint32
	idleTimeoutMs          uint32
	trIdleTimeoutMs        uint32
	maxRequestsPerChild    uint32
	maxLifespanPerChild    uint32
	satRecoverThresholdMs  uint32
	satRecoverThrottleRate uint32
}

var gAppConfig *Config
var gOpsConfig *OpsConfig

// GetConfig returns the application config
func GetConfig() *Config {
	return gAppConfig
}

func parseMapStrStr(encoded string) map[string]string {
	var m map[string]string
	var ss []string

	m = make(map[string]string)
	if len(encoded) > 0 {
		s := encoded
		ss = strings.Split(s, ",")
		for _, pair := range ss {
			z := strings.Split(pair, ":")
			if len(z) < 2 {
				logger.GetLogger().Log(logger.Alert, "could not parse pair", z)
				continue
			}
			m[z[0]] = z[1]
		}
	}
	return m
}

// InitConfig initializes the configuration, both the static configuration (from hera.txt) and the dynamic configuration
func InitConfig(poolName string) error {
	currentDir, abserr := filepath.Abs(filepath.Dir(os.Args[0]))

	if abserr != nil {
		currentDir = "./"
	} else {
		currentDir = currentDir + "/"
	}
	filename := currentDir + "hera.txt"

	cdb, err := config.NewTxtConfig(filename)
	if err != nil {
		return err
	}

	gAppConfig = &Config{numWorkersCh: make(chan int, 1)}

	logFile := cdb.GetOrDefaultString("log_file", "hera.log")
	logFile = currentDir + logFile
	logLevel := cdb.GetOrDefaultInt("log_level", logger.Info)

	err = logger.CreateLogger(logFile, "PROXY", int32(logLevel))
	if err != nil {
		FullShutdown()
	}

	gAppConfig.ChildExecutable = cdb.GetOrDefaultString("child.executable", "")
	gAppConfig.Port, err = cdb.GetInt("bind_port")
	if err != nil {
		return errors.New("Config error: bind_port undefined")
	}
	gAppConfig.CertChainFile = cdb.GetOrDefaultString("cert_chain_file", "")
	gAppConfig.KeyFile = cdb.GetOrDefaultString("key_file", "")

	gAppConfig.LifoScheduler = cdb.GetOrDefaultBool("lifo_scheduler_enabled", true)

	gAppConfig.NumStdbyDbs, err = cdb.GetInt("num_standby_dbs")
	if err != nil {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "failed to get num_standby_dbs from hera.txt, defaulting to 0")
		}
		//
		// @TODO get from twotask env variable
		//
		gAppConfig.NumStdbyDbs = 0
	}

	gAppConfig.ConfigReloadTimeMs = cdb.GetOrDefaultInt("config_reload_time_ms", 30*1000)
	gAppConfig.ConfigLoggingReloadTimeHours = cdb.GetOrDefaultInt("config_logging_reload_time_hours", 24)
	gAppConfig.CustomAuthTimeoutMs = cdb.GetOrDefaultInt("custom_auth_timeout", 1000)
	gAppConfig.TimeSkewThresholdWarnSec = cdb.GetOrDefaultInt("time_skew_threshold_warn", 2)
	gAppConfig.TimeSkewThresholdErrorSec = cdb.GetOrDefaultInt("time_skew_threshold_error", 15)
	gAppConfig.StrandedWorkerTimeoutMs = cdb.GetOrDefaultInt("max_stranded_time_interval", 2000)
	gAppConfig.HighLoadStrandedWorkerTimeoutMs = cdb.GetOrDefaultInt("high_load_max_stranded_time_interval", 600111)
	gAppConfig.HighLoadSkipInitiateRecoverPct = cdb.GetOrDefaultInt("high_load_skip_initiate_recover_pct", 80)
	gAppConfig.HighLoadPct = cdb.GetOrDefaultInt("high_load_pct", 130)   // >100 disabled
	gAppConfig.InitLimitPct = cdb.GetOrDefaultInt("init_limit_pct", 125) // >100 disabled

	gAppConfig.StateLogInterval = cdb.GetOrDefaultInt("state_log_interval", 1)
	if gAppConfig.StateLogInterval <= 0 {
		gAppConfig.StateLogInterval = 1
	}

	databaseType := cdb.GetOrDefaultString(ConfigDatabaseType, "oracle")
	if strings.EqualFold(databaseType, "oracle") {
		gAppConfig.DatabaseType = Oracle
		if gAppConfig.ChildExecutable == "" {
			gAppConfig.ChildExecutable = "oracleworker"
		}
	} else if strings.EqualFold(databaseType, "mysql") {
		gAppConfig.DatabaseType = MySQL
		if gAppConfig.ChildExecutable == "" {
			gAppConfig.ChildExecutable = "mysqlworker"
		}
	} else if strings.EqualFold(databaseType, "postgres") {
		gAppConfig.DatabaseType = POSTGRES
		if gAppConfig.ChildExecutable == "" {
			gAppConfig.ChildExecutable = "postgresworker"
		}
	} else {
		// db type is not supported
		return errors.New("database type must be either Oracle or MySQL")
	}

	gAppConfig.EnableSharding = cdb.GetOrDefaultBool("enable_sharding", false)

	gAppConfig.UseShardMap = cdb.GetOrDefaultBool("use_shardmap", true)
	gAppConfig.NumOfShards = cdb.GetOrDefaultInt("num_shards", 1)
	if gAppConfig.EnableSharding == false || gAppConfig.UseShardMap == false {
		gAppConfig.NumOfShards = 1
	}
	if (gAppConfig.NumOfShards < 1) || (gAppConfig.NumOfShards > 48) {
		return errors.New("num_shards must be between 1 and 48")
	}
	gAppConfig.ShardKeyName = strings.ToLower(cdb.GetOrDefaultString("shard_key_name", ""))
	gAppConfig.MaxScuttleBuckets = cdb.GetOrDefaultInt("max_scuttle", 1024)
	if (gAppConfig.MaxScuttleBuckets < 1) || (gAppConfig.MaxScuttleBuckets > 1024) {
		return errors.New("max_scuttle must be between 1 and 1024")
	}
	gAppConfig.ScuttleColName = cdb.GetOrDefaultString("scuttle_col_name", "scuttle_id")
	if len(gAppConfig.ScuttleColName) == 0 {
		return errors.New("scuttle_col_name is empty string")
	}
	algo := cdb.GetOrDefaultString("sharding_algo", "hash")
	algo = strings.ToUpper(algo)
	if algo == "HASH" {
		gAppConfig.ShardingAlgoHash = true
	} else {
		if algo == "MOD" {
			gAppConfig.ShardingAlgoHash = false
		} else {
			return errors.New("sharding_algo must be either hash or mod")
		}
	}
	gAppConfig.ShardingPostfix = cdb.GetOrDefaultString("sharding_postfix", "")
	gAppConfig.EnableWhitelistTest = cdb.GetOrDefaultBool("enable_whitelist_test", false)
	if gAppConfig.EnableWhitelistTest {
		gAppConfig.NumWhitelistChildren = cdb.GetOrDefaultInt("whitelist_children", 5)
	}
	gAppConfig.ShardingCfgReloadInterval = cdb.GetOrDefaultInt("sharding_cfg_reload_interval", 2)
	gAppConfig.ShardingCrossKeysErr = cdb.GetOrDefaultBool("sharding_cross_keys_err", false)
	gAppConfig.ShardKeyValueTypeIsString = cdb.GetOrDefaultBool("shard_key_value_type_is_string", false)

	gAppConfig.HostnamePrefix = parseMapStrStr(cdb.GetOrDefaultString("hostname_prefix", ""))

	gAppConfig.EnableCmdClientInfoToWorker = cdb.GetOrDefaultBool("enable_client_info_to_worker", false)

	gAppConfig.CfgFromTns = cdb.GetOrDefaultBool("cfg_from_tns", true)
	gAppConfig.CfgFromTnsOverrideNumShards = cdb.GetOrDefaultInt("cfg_from_tns_override_num_shards", -1)
	gAppConfig.CfgFromTnsOverrideTaf = cdb.GetOrDefaultInt("cfg_from_tns_override_taf", -1)
	gAppConfig.CfgFromTnsOverrideRWSplit = cdb.GetOrDefaultInt("cfg_from_tns_override_rw_split", -1)

	// TAF stuff
	gAppConfig.EnableTAF = cdb.GetOrDefaultBool("enable_taf", false)
	gAppConfig.TAFTimeoutMs = uint32(cdb.GetOrDefaultInt("taf_timeout_ms", 200))
	gAppConfig.TAFBinDuration = cdb.GetOrDefaultInt("taf_bin_duration", 3600*24)
	gAppConfig.TAFAllowSlowEveryX = cdb.GetOrDefaultInt("taf_allow_slow_every_x", 100)
	gAppConfig.TAFNormallySlowCount = cdb.GetOrDefaultInt("taf_normally_slow_count", 5)
	if gAppConfig.EnableTAF {
		InitTAF(gAppConfig.NumOfShards)
	}
	// TODO:
	gAppConfig.NumStdbyDbs = 1

	// Fetch Oracle worker configurations.. The defaults must be same between oracle worker and here for accurate logging.
	gAppConfig.EnableCache = cdb.GetOrDefaultBool("enable_cache", false)
	gAppConfig.EnableHeartBeat = cdb.GetOrDefaultBool("enable_heart_beat", false)
	gAppConfig.EnableQueryReplaceNL = cdb.GetOrDefaultBool("enable_query_replace_nl", true)
	gAppConfig.EnableBindHashLogging = cdb.GetOrDefaultBool("enable_bind_hash_logging", false)
	gAppConfig.EnableSessionVariables = cdb.GetOrDefaultBool("enable_session_variables", false)
	gAppConfig.UseNonBlocking = cdb.GetOrDefaultBool("use_non_blocking", false)

	var numWorkers int
	numWorkers = 6
	//err = config.InitOpsConfigWithName("../opscfg/hera.txt")
	err = config.InitOpsConfig()
	if err != nil {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "Error initializing ops config:", err.Error())
		}
	} else {
		cfg := config.GetOpsConfig()
		numWorkersOpscfg, err := cfg.GetInt(ConfigMaxWorkers)
		if err == nil {
			numWorkers = numWorkersOpscfg
		} // continue on error
		gOpsConfig = &OpsConfig{
			logLevel:               cfg.GetOrDefaultInt("log_level", logLevel),
			numWorkers:             uint32(numWorkers),
			idleTimeoutMs:          uint32(cfg.GetOrDefaultInt("idle_timeout_ms", 600000)),
			trIdleTimeoutMs:        uint32(cfg.GetOrDefaultInt("transaction_idle_timeout_ms", 900000)),
			maxLifespanPerChild:    uint32(cfg.GetOrDefaultInt("max_lifespan_per_child", 0)),
			maxRequestsPerChild:    uint32(cfg.GetOrDefaultInt("max_requests_per_child", 0)),
			satRecoverThresholdMs:  uint32(cfg.GetOrDefaultInt("saturation_recover_threshold", 200)),
			satRecoverThrottleRate: uint32(cfg.GetOrDefaultInt("saturation_recover_throttle_rate", 0)),
		}
		logger.SetLogVerbosity(int32(gOpsConfig.logLevel))
		gAppConfig.numWorkersCh <- numWorkers
	}

	gAppConfig.ReadonlyPct = cdb.GetOrDefaultInt("readonly_children_pct", 0)
	gAppConfig.TafChildrenPct = cdb.GetOrDefaultInt("taf_children_pct", 100)
	gAppConfig.InitialMaxChildren = numWorkers
	if gAppConfig.EnableWhitelistTest {
		if gAppConfig.NumWhitelistChildren < 2 {
			gAppConfig.NumWhitelistChildren = 2
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "whitelist_children must be >= 2, using 2")
			}
		}
		if gAppConfig.NumWhitelistChildren > gAppConfig.InitialMaxChildren {
			gAppConfig.NumWhitelistChildren = gAppConfig.InitialMaxChildren
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "whitelist_children must be < max_workers, using max_workers")
			}
		}
	}

	gAppConfig.BacklogPct = cdb.GetOrDefaultInt("backlog_pct", 30)
	gAppConfig.BacklogTimeoutMsec = cdb.GetOrDefaultInt("request_backlog_timeout", 1000)
	gAppConfig.ShortBacklogTimeoutMsec = cdb.GetOrDefaultInt("short_backlog_timeout", 30)
	gAppConfig.BacklogTimeoutUnit = int64(gAppConfig.BacklogTimeoutMsec) / 5
	if gAppConfig.BacklogTimeoutMsec < 5 {
		gAppConfig.BacklogTimeoutUnit = 1
	}
	gAppConfig.SoftEvictionEffectiveTimeMs = cdb.GetOrDefaultInt("soft_eviction_effective_time", 10000)
	gAppConfig.SoftEvictionProbability = cdb.GetOrDefaultInt("soft_eviction_probability", 50)
	gAppConfig.BindEvictionTargetConnPct = cdb.GetOrDefaultInt("bind_eviction_target_conn_pct", 50)
	gAppConfig.BindEvictionMaxThrottle = cdb.GetOrDefaultInt("bind_eviction_max_throttle", 20)
	default_evict_names := fmt.Sprintf("id,num,%s", SrcPrefixAppKey)
	gAppConfig.BindEvictionNames = cdb.GetOrDefaultString("bind_eviction_names", default_evict_names)
	gAppConfig.BindEvictionThresholdPct = cdb.GetOrDefaultInt("bind_eviction_threshold_pct", 60)
	fmt.Sscanf(cdb.GetOrDefaultString("bind_eviction_decr_per_sec", "10.0"),
		"%f", &gAppConfig.BindEvictionDecrPerSec)

	gAppConfig.SkipEvictRegex = cdb.GetOrDefaultString("skip_eviction_host_prefix", "")
	gAppConfig.EvictRegex = cdb.GetOrDefaultString("eviction_host_prefix", "")

	gAppConfig.BouncerEnabled = cdb.GetOrDefaultBool("bouncer_enabled", true)
	gAppConfig.BouncerStartupDelay = cdb.GetOrDefaultInt("bouncer_startup_delay", 10)
	gAppConfig.BouncerPollInterval = cdb.GetOrDefaultInt("bouncer_poll_interval_ms", 100)
	gAppConfig.EnableProfile = cdb.GetOrDefaultBool("enable_profile", false)
	gAppConfig.ProfileHTTPPort = cdb.GetOrDefaultString("profile_http_port", "6060")
	gAppConfig.ProfileTelnetPort = cdb.GetOrDefaultString("profile_telnet_port", "3030")
	gAppConfig.UseOpenSSL = cdb.GetOrDefaultBool("openssl", false)
	gAppConfig.MuxPidFile = cdb.GetOrDefaultString("mux_pid_file", "mux.pid")

	gAppConfig.ErrorCodePrefix = cdb.GetOrDefaultString("error_code_prefix", "HERA")
	gAppConfig.StateLogPrefix = cdb.GetOrDefaultString("state_log_prefix", "hera")
	gAppConfig.ManagementTablePrefix = cdb.GetOrDefaultString("management_table_prefix", "hera")
	gAppConfig.RacMaintReloadInterval = cdb.GetOrDefaultInt("rac_sql_interval", 10)
	gAppConfig.RacRestartWindow = cdb.GetOrDefaultInt("rac_restart_window", 240)
	gAppConfig.lifeSpanCheckInterval = cdb.GetOrDefaultInt("lifespan_check_interval", 10)

	gAppConfig.EnableConnLimitCheck = cdb.GetOrDefaultBool("enable_connlimit_check", false)
	gAppConfig.EnableQueryBindBlocker = cdb.GetOrDefaultBool("enable_query_bind_blocker", false)
	gAppConfig.QueryBindBlockerMinSqlPrefix = cdb.GetOrDefaultInt("query_bind_blocker_min_sql_prefix", 20)
	gAppConfig.TestingEnableDMLTaf = cdb.GetOrDefaultBool("testing_enable_dml_taf", false)
	gAppConfig.EnableDanglingWorkerRecovery = cdb.GetOrDefaultBool("enable_danglingworker_recovery", false)

	gAppConfig.GoStatsInterval = cdb.GetOrDefaultInt("go_stats_interval", 10)
	defaultConns := 10000 // disable by default
	if gAppConfig.EnableTAF {
		defaultConns = 5
	}
	gAppConfig.RandomStartMs = cdb.GetOrDefaultInt("random_start_ms", 20000)
	gAppConfig.MaxDbConnectsPerSec = cdb.GetOrDefaultInt("max_db_connects_per_sec", defaultConns)
	gAppConfig.MaxDesiredHealthyWorkerPct = cdb.GetOrDefaultInt("max_desire_healthy_worker_pct", 90)
	if gAppConfig.MaxDesiredHealthyWorkerPct > 100 {
		gAppConfig.MaxDesiredHealthyWorkerPct = 90
	}

	//Initialize OTEL configs
	initializeOTELConfigs(cdb, poolName)
	if logger.GetLogger().V(logger.Info) {
		otelconfig.OTelConfigData.Dump()
	}
	return nil
}

// This function takes care of initialize OTEL configuration
func initializeOTELConfigs(cdb config.Config, poolName string) {
	otelconfig.OTelConfigData = &otelconfig.OTelConfig{}
	//TODO initialize the values
	otelconfig.OTelConfigData.Enabled = cdb.GetOrDefaultBool("enable_otel", false)
	otelconfig.OTelConfigData.SkipCalStateLog = cdb.GetOrDefaultBool("skip_cal_statelog", false)
	otelconfig.OTelConfigData.MetricNamePrefix = cdb.GetOrDefaultString("otel_metric_prefix", "pp.occ")
	otelconfig.OTelConfigData.Host = cdb.GetOrDefaultString("otel_agent_host", "localhost")
	otelconfig.OTelConfigData.MetricsPort = cdb.GetOrDefaultInt("otel_agent_metrics_port", 4318)
	otelconfig.OTelConfigData.TracePort = cdb.GetOrDefaultInt("otel_agent_trace_port", 4318)
	otelconfig.OTelConfigData.OtelMetricGRPC = cdb.GetOrDefaultBool("otel_agent_use_grpc_metric", false)
	otelconfig.OTelConfigData.OtelTraceGRPC = cdb.GetOrDefaultBool("otel_agent_use_grpc_trace", false)
	otelconfig.OTelConfigData.MetricsURLPath = cdb.GetOrDefaultString("otel_agent_metrics_uri", "")
	otelconfig.OTelConfigData.TraceURLPath = cdb.GetOrDefaultString("otel_agent_trace_uri", "")
	otelconfig.OTelConfigData.PoolName = poolName
	otelconfig.OTelConfigData.UseTls = cdb.GetOrDefaultBool("otel_use_tls", false)
	otelconfig.OTelConfigData.TLSCertPath = cdb.GetOrDefaultString("otel_tls_cert_path", "")
	otelconfig.OTelConfigData.ResolutionTimeInSec = cdb.GetOrDefaultInt("otel_resolution_time_in_sec", 1)
	otelconfig.OTelConfigData.ExporterTimeout = cdb.GetOrDefaultInt("otel_exporter_time_in_sec", 30)
	otelconfig.OTelConfigData.EnableRetry = cdb.GetOrDefaultBool("otel_enable_exporter_retry", false)
	otelconfig.OTelConfigData.ResourceType = gAppConfig.StateLogPrefix
	otelconfig.OTelConfigData.OTelErrorReportingInterval = cdb.GetOrDefaultInt("otel_error_reporting_interval_in_sec", 60)
	otelconfig.SetOTelIngestToken(cdb.GetOrDefaultString("otel_ingest_token", ""))
}

func LogOccConfigs() {
	whiteListConfigs := map[string]map[string]interface{}{
		"BACKLOG": {
			"backlog_pct":             gAppConfig.BacklogPct,
			"request_backlog_timeout": gAppConfig.BacklogTimeoutMsec,
			"short_backlog_timeout":   gAppConfig.ShortBacklogTimeoutMsec,
		},
		"BOUNCER": {
			"bouncer_enabled":          gAppConfig.BouncerEnabled,
			"bouncer_startup_delay":    gAppConfig.BouncerStartupDelay,
			"bouncer_poll_interval_ms": gAppConfig.BouncerPollInterval,
		},
		"OTEL": {
			"enable_otel":                          otelconfig.OTelConfigData.Enabled,
			"otel_use_tls":                         otelconfig.OTelConfigData.UseTls,
			"skip_cal_statelog":                    otelconfig.OTelConfigData.SkipCalStateLog,
			"otel_agent_host":                      otelconfig.OTelConfigData.Host,
			"otel_agent_metrics_port":              otelconfig.OTelConfigData.MetricsPort,
			"otel_agent_trace_port":                otelconfig.OTelConfigData.TracePort,
			"otel_agent_metrics_uri":               otelconfig.OTelConfigData.MetricsURLPath,
			"otel_agent_trace_uri":                 otelconfig.OTelConfigData.TraceURLPath,
			"otel_resolution_time_in_sec":          otelconfig.OTelConfigData.ResolutionTimeInSec,
			"otel_error_reporting_interval_in_sec": otelconfig.OTelConfigData.OTelErrorReportingInterval,
		},
		"PROFILE": {
			"enable_profile":      gAppConfig.EnableProfile,
			"profile_http_port":   gAppConfig.ProfileHTTPPort,
			"profile_telnet_port": gAppConfig.ProfileTelnetPort,
		},
		"SHARDING": {
			"enable_sharding":                gAppConfig.EnableSharding,
			"use_shardmap":                   gAppConfig.UseShardMap,
			"num_shards":                     gAppConfig.NumOfShards,
			"shard_key_name":                 gAppConfig.ShardKeyName,
			"max_scuttle":                    gAppConfig.MaxScuttleBuckets,
			"scuttle_col_name":               gAppConfig.ScuttleColName,
			"shard_key_value_type_is_string": gAppConfig.ShardKeyValueTypeIsString,
			"enable_whitelist_test":          gAppConfig.EnableWhitelistTest,
			"whitelist_children":             gAppConfig.NumWhitelistChildren,
			"sharding_postfix":               gAppConfig.ShardingPostfix,
			"sharding_cfg_reload_interval":   gAppConfig.ShardingCfgReloadInterval,
			"hostname_prefix":                gAppConfig.HostnamePrefix,
			"sharding_cross_keys_err":        gAppConfig.ShardingCrossKeysErr,
			//"enable_sql_rewrite", // not found anywhere?
			"sharding_algo": gAppConfig.ShardingAlgoHash,
		},
		"TAF": {
			"enable_taf":              gAppConfig.EnableTAF,
			"testing_enable_dml_taf":  gAppConfig.TestingEnableDMLTaf,
			"taf_timeout_ms":          gAppConfig.TAFTimeoutMs,
			"taf_bin_duration":        gAppConfig.TAFBinDuration,
			"taf_allow_slow_every_x":  gAppConfig.TAFAllowSlowEveryX,
			"taf_normally_slow_count": gAppConfig.TAFNormallySlowCount,
		},
		"BIND-EVICTION": {
			"child.executable": gAppConfig.ChildExecutable,
			//"enable_bind_hash_logging" FOUND FOR SOME OCCs ONLY IN occ.def
			"bind_eviction_threshold_pct":       gAppConfig.BindEvictionThresholdPct,
			"bind_eviction_decr_per_sec":        gAppConfig.BindEvictionDecrPerSec,
			"bind_eviction_target_conn_pct":     gAppConfig.BindEvictionTargetConnPct,
			"bind_eviction_max_throttle":        gAppConfig.BindEvictionMaxThrottle,
			"bind_eviction_names":               gAppConfig.BindEvictionNames,
			"skip_eviction_host_prefix":         gAppConfig.SkipEvictRegex,
			"eviction_host_prefix":              gAppConfig.EvictRegex,
			"query_bind_blocker_min_sql_prefix": gAppConfig.QueryBindBlockerMinSqlPrefix,
			"enable_connlimit_check":            gAppConfig.EnableConnLimitCheck,
		},
		"MANUAL-RATE-LIMITER": {
			"enable_query_bind_blocker": gAppConfig.EnableQueryBindBlocker,
		},
		"SATURATION-RECOVERY": {
			"saturation_recover_threshold":     GetSatRecoverThresholdMs(),
			"saturation_recover_throttle_rate": GetSatRecoverThrottleRate(),
		},
		"SOFT-EVICTION": {
			"soft_eviction_effective_time": gAppConfig.SoftEvictionEffectiveTimeMs,
			"soft_eviction_probability":    gAppConfig.SoftEvictionProbability,
		},
		"WORKER-CONFIGURATIONS": {
			"lifespan_check_interval": gAppConfig.lifeSpanCheckInterval,
			"lifo_scheduler_enabled":  gAppConfig.LifoScheduler,
			//"num_workers_per_proxy",  // only present in occ.def for some occs
			//"max_clients_per_worker", // only present in occ.def for some occs
			"max_stranded_time_interval":           gAppConfig.StrandedWorkerTimeoutMs,
			"high_load_max_stranded_time_interval": gAppConfig.HighLoadStrandedWorkerTimeoutMs,
			"high_load_skip_initiate_recover_pct":  gAppConfig.HighLoadSkipInitiateRecoverPct,
			"enable_danglingworker_recovery":       gAppConfig.EnableDanglingWorkerRecovery,
			"max_db_connects_per_sec":              gAppConfig.MaxDbConnectsPerSec,
			"max_lifespan_per_child":               GetMaxLifespanPerChild(),
			"max_requests_per_child":               GetMaxRequestsPerChild(),
			"max_desire_healthy_worker_pct":        gAppConfig.MaxDesiredHealthyWorkerPct,
		},
		"R-W-SPLIT": {
			"readonly_children_pct": gAppConfig.ReadonlyPct,
		},
		"RAC": {
			"management_table_prefix": gAppConfig.ManagementTablePrefix,
			"rac_sql_interval":        gAppConfig.RacMaintReloadInterval,
			"rac_restart_window":      gAppConfig.RacRestartWindow,
		},
		"GENERAL-CONFIGURATIONS": {
			"database_type":   gAppConfig.DatabaseType, //	Oracle = 0; MySQL=1; POSTGRES=2
			"log_level":       gOpsConfig.logLevel,
			"high_load_pct":   gAppConfig.HighLoadPct,
			"init_limit_pct":  gAppConfig.InitLimitPct,
			"num_standby_dbs": gAppConfig.NumStdbyDbs,
		},
		"ENABLE_CFG_FROM_TNS": {
			"cfg_from_tns":                     gAppConfig.CfgFromTns,
			"cfg_from_tns_override_num_shards": gAppConfig.CfgFromTnsOverrideNumShards,
			"cfg_from_tns_override_taf":        gAppConfig.CfgFromTnsOverrideTaf,
			"cfg_from_tns_override_rw_split":   gAppConfig.CfgFromTnsOverrideRWSplit,
		},
		"STATEMENT-CACHE": {
			"enable_cache":            gAppConfig.EnableCache,
			"enable_heart_beat":       gAppConfig.EnableHeartBeat,
			"enable_query_replace_nl": gAppConfig.EnableQueryReplaceNL,
		},
		"SESSION-VARIABLES": {
			"enable_session_variables": gAppConfig.EnableSessionVariables,
		},
		"BIND-HASH-LOGGING": {
			"enable_bind_hash_logging": gAppConfig.EnableBindHashLogging,
		},
		"KEEP-ALIVE": {
			"use_non_blocking": gAppConfig.UseNonBlocking,
		},
	}
	calName := mux_config_cal_name
	for feature, configs := range whiteListConfigs {
		switch feature {
		case "BACKLOG":
			if gAppConfig.BacklogPct == 0 {
				continue
			}
		case "BOUNCER":
			if !gAppConfig.BouncerEnabled {
				continue
			}
		case "OTEL":
			if !otelconfig.OTelConfigData.Enabled {
				continue
			}
		case "PROFILE":
			if !gAppConfig.EnableProfile {
				continue
			}
		case "SHARDING":
			if !gAppConfig.EnableSharding {
				continue
			}
		case "TAF":
			if !gAppConfig.EnableTAF {
				continue
			}
		case "R-W-SPLIT":
			if gAppConfig.ReadonlyPct == 0 {
				continue
			}
		case "SATURATION-RECOVERY", "BIND-EVICTION":
			if GetSatRecoverThrottleRate() <= 0 {
				continue
			}
		case "SOFT-EVICTION":
			if GetSatRecoverThrottleRate() <= 0 && gAppConfig.SoftEvictionProbability <= 0 {
				continue
			}
		case "MANUAL-RATE-LIMITER":
			if !gAppConfig.EnableQueryBindBlocker {
				continue
			}
		case "ENABLE_CFG_FROM_TNS":
			if !gAppConfig.CfgFromTns {
				continue
			}
		case "STATEMENT-CACHE":
			if !gAppConfig.EnableCache {
				continue
			}
			calName = oracle_worker_config_cal_name
		case "SESSION-VARIABLES":
			if !gAppConfig.EnableSessionVariables {
				continue
			}
			calName = oracle_worker_config_cal_name
		case "BIND-HASH-LOGGING":
			if !gAppConfig.EnableBindHashLogging {
				continue
			}
			calName = oracle_worker_config_cal_name
		case "KEEP-ALIVE":
			if !gAppConfig.UseNonBlocking {
				continue
			}
			calName = oracle_worker_config_cal_name
		}

		evt := cal.NewCalEvent(calName, fmt.Sprintf(feature), cal.TransOK, "")
		for cfg, val := range configs {
			s := fmt.Sprintf("%v", val)
			evt.AddDataStr(cfg, s)
		}
		evt.Completed()
	}
}

// CheckOpsConfigChange checks if the ops config file needs to be reloaded and reloads it if necessary.
// it is called every several seconds from a dedicated go-routine.
func CheckOpsConfigChange() {
	cfg := config.GetOpsConfig()
	if cfg.Changed() {
		err := cfg.Load()
		if err != nil {
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "Error loading ops config:", err.Error())
			}
		} else {
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "Loading ops config")
			}
			logLevel, err := cfg.GetInt("log_level")
			if (err == nil) && (logLevel != gOpsConfig.logLevel) {
				logger.SetLogVerbosity(int32(logLevel))
				gOpsConfig.logLevel = logLevel
			}

			idleTimeoutMs := uint32(cfg.GetOrDefaultInt("idle_timeout_ms", 600000))
			trIdleTimeoutMs := uint32(cfg.GetOrDefaultInt("transaction_idle_timeout_ms", 900000))
			if idleTimeoutMs != gOpsConfig.idleTimeoutMs {
				atomic.StoreUint32(&(gOpsConfig.idleTimeoutMs), idleTimeoutMs)
			}
			if trIdleTimeoutMs != gOpsConfig.trIdleTimeoutMs {
				atomic.StoreUint32(&(gOpsConfig.trIdleTimeoutMs), trIdleTimeoutMs)
			}

			maxLifespanPerChild := uint32(cfg.GetOrDefaultInt("max_lifespan_per_child", 0))
			maxRequestsPerChild := uint32(cfg.GetOrDefaultInt("max_requests_per_child", 0))
			if maxLifespanPerChild != gOpsConfig.maxLifespanPerChild {
				atomic.StoreUint32(&(gOpsConfig.maxLifespanPerChild), maxLifespanPerChild)
			}
			if maxRequestsPerChild != gOpsConfig.maxRequestsPerChild {
				atomic.StoreUint32(&(gOpsConfig.maxRequestsPerChild), maxRequestsPerChild)
			}

			satRecoverThresholdMs := uint32(cfg.GetOrDefaultInt("saturation_recover_threshold", 200))
			if satRecoverThresholdMs != gOpsConfig.satRecoverThresholdMs {
				atomic.StoreUint32(&(gOpsConfig.satRecoverThresholdMs), satRecoverThresholdMs)
			}
			satRecoverThrottleRate := uint32(cfg.GetOrDefaultInt("saturation_recover_throttle_rate", 0))
			if satRecoverThrottleRate != gOpsConfig.satRecoverThrottleRate {
				atomic.StoreUint32(&(gOpsConfig.satRecoverThrottleRate), satRecoverThrottleRate)
			}

			numWorkers, err := cfg.GetInt(ConfigMaxWorkers)
			if err != nil {
				if logger.GetLogger().V(logger.Alert) {
					logger.GetLogger().Log(logger.Alert, "Error reading max_connections in opscfg reload", err.Error())
				}
			} else {
				if uint32(numWorkers) != gOpsConfig.numWorkers {
					if logger.GetLogger().V(logger.Info) {
						logger.GetLogger().Log(logger.Info, "Changing max_connections from", gOpsConfig.numWorkers, "to", numWorkers)
					}
					atomic.StoreUint32(&(gOpsConfig.numWorkers), uint32(numWorkers))
					gAppConfig.numWorkersCh <- numWorkers
				} else {
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, "max_connections unchanged in opsconfig")
					}
				}
			}
		}
	}
}

// GetIdleTimeoutMs gets the idle timeout for the client connections. A client connection is terminated if it is idle for more that this
func GetIdleTimeoutMs() int {
	return int(atomic.LoadUint32(&(gOpsConfig.idleTimeoutMs)))
}

// GetTrIdleTimeoutMs gets the idle timeout for the client connections when they are in a transaction. A client connection is terminated if it is idle for more that this
func GetTrIdleTimeoutMs() int {
	return int(atomic.LoadUint32(&(gOpsConfig.trIdleTimeoutMs)))
}

// GetMaxLifespanPerChild returns how much time a worker process is allowed to run. After this time, a worker is killed and a new one is restarted
func GetMaxLifespanPerChild() uint32 {
	return atomic.LoadUint32(&(gOpsConfig.maxLifespanPerChild))
}

// GetMaxRequestsPerChild is similar to GetMaxLifespanPerChild, it returns how many requests a worker will server, before is re-started.
func GetMaxRequestsPerChild() uint32 {
	return atomic.LoadUint32(&(gOpsConfig.maxRequestsPerChild))
}

// NumWorkersCh returns the channel where number of workers change is sent
func (cfg *Config) NumWorkersCh() <-chan int {
	return cfg.numWorkersCh
}

// GetBacklogLimit returns the limit for the number of backlogged workers for a certain pool and shard.
func (cfg *Config) GetBacklogLimit(wtype HeraWorkerType, shard int) int {
	if wtype == wtypeRO {
		return gAppConfig.BacklogPct * GetNumRWorkers(shard) / 100
	} else if wtype == wtypeStdBy {
		return gAppConfig.BacklogPct * GetNumStdByWorkers(shard) / 100
	}
	return gAppConfig.BacklogPct * GetNumWWorkers(shard) / 100
}

// GetSatRecoverThresholdMs gets the saturation recover threshold in milliseconds from ops config
func GetSatRecoverThresholdMs() uint32 {
	//
	// if config value was negative, this will return a hugh uint32 that effectively disables sat.
	//
	return atomic.LoadUint32(&(gOpsConfig.satRecoverThresholdMs))
}

// GetSatRecoverThrottleRate gets the saturation recover throttle rate from ops config
func GetSatRecoverThrottleRate() uint32 {
	cdbval := atomic.LoadUint32(&(gOpsConfig.satRecoverThrottleRate))
	//
	// negative config value will be casted into a uint32 > 100.
	//
	if cdbval > 100 {
		cdbval = 0
	}
	return cdbval
}

// GetSatRecoverFreqMs gets the saturation recover frequency in milliseconds from ops config
func GetSatRecoverFreqMs(shard int) int {
	trate := int(GetSatRecoverThrottleRate())
	numWorkers := GetNumWorkers(shard)
	if (trate == 0) || (numWorkers == 0) {
		return int(^uint(0) >> 1) // INT_MAX
	}
	// can we just do (100000.0 / chunk)
	return int(1000.0 / (float32(trate*numWorkers) / 100.0))
}

// GetSatRecoverThrottleCnt gets the saturation recover throttle count
func GetSatRecoverThrottleCnt(shard int) int {
	return int(float32(int(GetSatRecoverThrottleRate())*GetNumWorkers(shard)) / 100.0)
}

// GetWhiteListChildCount gets the number of whitelist children for a shard
func GetWhiteListChildCount(shard int) int {
	if (shard > 0) && gAppConfig.EnableWhitelistTest {
		return gAppConfig.NumWhitelistChildren
	}
	return 0
}

// GetNumWorkers gets the number of children for a shard.
func GetNumWorkers(shard int) int {
	numWhiteList := GetWhiteListChildCount(shard)
	if numWhiteList > 0 {
		return numWhiteList
	}
	return int(atomic.LoadUint32(&(gOpsConfig.numWorkers)))
}

// GetNumRWorkers gets the number of workers for the "Read" pool
func GetNumRWorkers(shard int) int {
	numWhiteList := GetWhiteListChildCount(shard)
	if (numWhiteList > 0) && (gAppConfig.ReadonlyPct > 0) {
		// ReadonlyPct is not applied
		return numWhiteList
	}
	num := 0
	if gAppConfig.ReadonlyPct > 0 {
		num = GetNumWorkers(shard) * gAppConfig.ReadonlyPct / 100
		if num == 0 {
			num = 1
		}
	}
	return num
}

// GetNumStdByWorkers gets the number of workers for the "StdBy" pool
func GetNumStdByWorkers(shard int) int {
	num := GetNumWWorkers(shard)
	// TafChildrenPct should not be greater than 100.
	if gAppConfig.TafChildrenPct > 100 {
		return num
	}
	if gAppConfig.EnableTAF && gAppConfig.TafChildrenPct < 100 {
		if gAppConfig.TafChildrenPct < 0 {
			num = 1
		} else {
			num = num * gAppConfig.TafChildrenPct / 100
			if num == 0 {
				num = 1
			}
		}
	}
	return num
}

// GetNumWWorkers gets the number of workers for the "Write" pool
func GetNumWWorkers(shard int) int {
	numWhiteList := GetWhiteListChildCount(shard)
	if numWhiteList > 0 {
		return numWhiteList
	}
	num := GetNumWorkers(shard)
	if gAppConfig.ReadonlyPct > 0 {
		num = num - num*(gAppConfig.ReadonlyPct)/100
		if num == 0 {
			num = 1
		}
	}
	return num
}
