package config

import (
	"errors"
	"fmt"
	"github.com/paypal/hera/utility/logger"
	"sync/atomic"
)

var OTelConfigData *OTelConfig
var OTelIngestTokenData atomic.Value

// OTelConfig represent configuration related to OTEL collector to export data
type OTelConfig struct {
	MetricNamePrefix           string
	Host                       string
	HttpPort                   int
	GRPCPort                   int
	MetricsURLPath             string
	TraceURLPath               string
	PoolName                   string
	ResourceType               string
	Enabled                    bool
	SkipCalStateLog            bool
	ResolutionTimeInSec        int
	ExporterTimeout            int
	UseTls                     bool
	TLSCertPath                string
	UseOtelGRPC                bool
	OTelErrorReportingInterval int
	EnableRetry                bool
}

// Validation function to check whether pool name is configured or not
func (config *OTelConfig) validate() error {
	if len(config.PoolName) <= 0 {
		logger.GetLogger().Log(logger.Alert, "OTEL configurations validation failed, PoolName m=not configured")
		return errors.New("OTEL configurations validation failed, PoolName m=not configured")
	}
	return nil
}

func (config *OTelConfig) Dump() {
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("Host : %s", config.Host))
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("Http Port: %d", config.HttpPort))
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("GRPC Port: %d", config.GRPCPort))
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("Poolname: %s", config.PoolName))
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("ResolutionTimeInSec: %d", config.ResolutionTimeInSec))
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("UseTls: %t", config.UseTls))
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("UrlPath: %s", config.MetricsURLPath))
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("UseOtelGRPC: %t", config.UseOtelGRPC))
}

func (config *OTelConfig) PopulateMetricNamePrefix(metricName string) string {
	return fmt.Sprintf("%s.%s", config.MetricNamePrefix, metricName)
}

func SetOTelIngestToken(value string) {
	OTelIngestTokenData.Store(value)
}

func GetOTelIngestToken() string {
	return OTelIngestTokenData.Load().(string)
}