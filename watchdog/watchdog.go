package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/paypal/hera/lib"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/watchdoglib"
)

func main() {
	parentSignal := make(chan os.Signal, 1)
	signal.Ignore(syscall.SIGPIPE)
	signal.Notify(parentSignal, syscall.SIGTERM, syscall.SIGINT)
	namePtr := flag.String("name", "", "module name in v$session table")
	flag.Parse()

	if len(*namePtr) == 0 {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Info, "missing mandatory --name parameter")
		}
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("stopping watchdog process: %d as mandatory --name parameter not provided", os.Getgid()))
		os.Exit(1)
	}

	//Initialize the config internally it initialize logger pointing to hera.log or filename configured in confgiurations
	configerr := lib.InitConfig()
	if configerr != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "Watchdog: failed to initialize configuration:", configerr.Error())
		}
		os.Exit(1)
	}

	//Initialize Statelog in watchdog. So it has reference for fd in watchdog even if mux dies since watchdog has reference for statelog.
	//So statelog won't get exit even mux dies.
	lib.GetStateLog()

	//Getting occmux binary path
	currentDir, err := os.Getwd()
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "watchdog:Failed to fetch current working directory")
		os.Exit(1)
	}
	muxpath := fmt.Sprintf("%s/mux", currentDir)
	if !isExist(&muxpath) {
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("OCC Mux Process File path: %s doesn't exist", muxpath))
		os.Exit(1)
	}
	processList := [][]string{}
	muxProcess := []string{muxpath, "--name", *namePtr}
	processList = append(processList, muxProcess)
	logger.GetLogger().Log(logger.Info, "Starting watchdog process.")
	watcher := watchdoglib.NewWatchdog(processList)
	watcher.Start()
	// give it a second to get started
	time.Sleep(2 * time.Millisecond)
	select {
	case <-parentSignal:
		logger.GetLogger().Log(logger.Info, "Received Terminal Signal.")
		watcher.ReqStopWatchdog <- true
		logger.GetLogger().Log(logger.Info, "Watchdog process got killed, sent stop signal to its children process as well.")
	case <-watcher.Done:
		logger.GetLogger().Log(logger.Info, "Watchdog exited.")
	}
}

//Check whether path exist or not
func isExist(filePath *string) bool {
	_, err := os.Stat(*filePath)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}
