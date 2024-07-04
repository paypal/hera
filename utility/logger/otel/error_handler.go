package otel

import (
	"fmt"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type OTelErrorHandler struct{}

type OTelErrorData struct {
	err          error
	occurredTime int64
}

var (
	oTelErrorLoggingLock sync.Mutex
	errorTicker          *time.Ticker
	gErrorDataMap        atomic.Value
	logTickerInitialized atomic.Bool
)

// Handle function handles errors in async way whenever runtime error while publishing data to OTEL agent.
func (handler OTelErrorHandler) Handle(err error) {
	if err == nil {
		return
	}
	logger.GetLogger().Log(logger.Warning, fmt.Sprintf("otel publishing error %v", err))
	oTelErrorLoggingLock.Lock()
	defer oTelErrorLoggingLock.Unlock()
	errorDataMapVal := gErrorDataMap.Load()
	errorDataMap := errorDataMapVal.(map[string]*OTelErrorData)
	if errorDataMap == nil {
		errorDataMap = make(map[string]*OTelErrorData)
	}
	errorDataMap[reflect.TypeOf(err).String()] = &OTelErrorData{err: err, occurredTime: time.Now().Unix()}
	if !logTickerInitialized.Load() {
		handler.logOTelErrorCalEvent(errorDataMap)
		errorDataMap = make(map[string]*OTelErrorData) //Reinitialize the map after process it.
		gErrorDataMap.Store(errorDataMap)
		logTickerInitialized.Store(true)
	} else {
		gErrorDataMap.Store(errorDataMap)
	}
}

// logOTELErrorCalEvent Log CAL event peridiocally every 15 mins in case any issues with OTEL data publish
func (handler OTelErrorHandler) processOTelErrorsMap() {
	go func() {
		for {
			select {
			case <-errorTicker.C:
				oTelErrorLoggingLock.Lock()
				errorDataMapVal := gErrorDataMap.Load()
				errorDataMap := errorDataMapVal.(map[string]*OTelErrorData)
				if errorDataMap != nil && len(errorDataMap) > 0 {
					handler.logOTelErrorCalEvent(errorDataMap)
					errorDataMap = make(map[string]*OTelErrorData) //Reinitialize the map after process it.
					gErrorDataMap.Store(errorDataMap)
				}
				oTelErrorLoggingLock.Unlock()
			}
		}
	}()
}

// logOTelErrorCalEvent It takes of logging OTEL
func (handler OTelErrorHandler) logOTelErrorCalEvent(errorDataMap map[string]*OTelErrorData) {
	for _, errorData := range errorDataMap {
		event := cal.NewCalEvent("OTEL", "CONNECTION", "2", fmt.Sprintf("%v", errorData.err))
		event.AddDataInt("occurredTime", errorData.occurredTime)
		event.AddDataInt("loggedTime", time.Now().Unix())
		event.Completed()
	}
}
