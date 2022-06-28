package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/paypal/hera/config"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/watchdoglib"
)

const (
	LATEST_GO_VERSION = "1.18.2"
	OLD_GO_VERSION    = "1.10"
)

func main() {
	parentSignal := make(chan os.Signal, 1)
	signal.Ignore(syscall.SIGPIPE)
	signal.Notify(parentSignal, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	namePtr := flag.String("name", "", "module name in v$session table")
	flag.Parse()

	if len(*namePtr) == 0 {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "missing mandatory --name parameter")
		}
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("stopping watchdog process: %d as mandatory --name parameter not provided", os.Getgid()))
		os.Exit(1)
	}

	//Getting occmux binary path
	currentDir, abserr := filepath.Abs(filepath.Dir(os.Args[0]))
	if abserr != nil {
		currentDir = "./"
	} else {
		currentDir = currentDir + "/"
	}
	//Initialize config, so internaly it Initializes logger pointing to hera.log or filename configured in confgiurations.
	configData, configerr := initializeConfig()
	if configerr != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "watchdog: failed to initialize configuration:", configerr.Error())
		}
		os.Exit(1)
	}

	//Initialize Statelog in watchdog. watchdog will have reference for statelog's fd. So statelog won't get exit if mux dies.
	stateLogErr := initializeStateLog()
	if stateLogErr != nil {
		logger.GetLogger().Log(logger.Alert, "watchdog: failed to initialize statelog:", stateLogErr.Error())
	}

	muxpath := filepath.Join(currentDir, "mux")
	if !isExist(&muxpath) {
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("occ mux process file path: %s doesn't exist", muxpath))
		os.Exit(1)
	}
	//Set OCCAPP environment variable
	os.Setenv("OCC_NAME", *namePtr)
	//Write watchdog processId to a file
	pidErr := writePidToFile(currentDir, configData)
	if pidErr != nil {
		logger.GetLogger().Log(logger.Alert, "watchdog process already running:", pidErr.Error())
		os.Exit(1)
	}
	processList := [][]string{}
	muxProcess := []string{muxpath, "--name", *namePtr}
	processList = append(processList, muxProcess)
	logger.GetLogger().Log(logger.Alert, "Starting watchdog process.")
	watcher := watchdoglib.NewWatchdog(processList)
	//Start watcher
	watcher.Start()
	// give it a second to get started
	time.Sleep(2 * time.Millisecond)
	select {
	case <-parentSignal:
		logger.GetLogger().Log(logger.Alert, "received terminal signal.")
		watcher.ReqStopWatchdog <- true
		logger.GetLogger().Log(logger.Alert, "watchdog process got killed, sent stop signal to its children process as well.")
	case <-watcher.Done:
		logger.GetLogger().Log(logger.Alert, "watchdog exited.")
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

//Load configfile if it fails to load then return error
func initializeConfig() (config.Config, error) {
	currentDir, abserr := filepath.Abs(filepath.Dir(os.Args[0]))

	if abserr != nil {
		currentDir = "./"
	} else {
		currentDir = currentDir + "/"
	}

	filename := currentDir + "hera.txt"

	cdb, err := config.NewTxtConfig(filename)
	if err != nil {
		return nil, err
	}

	logFile := cdb.GetOrDefaultString("log_file", "hera.log")
	logFile = currentDir + logFile
	logLevel := cdb.GetOrDefaultInt("log_level", logger.Info)

	err = logger.CreateLogger(logFile, "PROXY", int32(logLevel))
	if err != nil {
		return nil, err
	}
	return cdb, nil
}

//Initializes state-log
func initializeStateLog() error {
	currentDir, absperr := filepath.Abs(filepath.Dir(os.Args[0]))
	if absperr != nil {
		currentDir = "./"
	} else {
		currentDir = currentDir + "/"
	}

	filename := currentDir + "state.log"

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	// format backward compatible with C++
	log.New(file, "" /*log.Ldate|log.Ltime*/, 0)
	return nil
}

//Write watchdog process details to a file
func writePidToFile(currentDir string, cdb config.Config) error {
	pidFile := cdb.GetOrDefaultString("pid_file", "occ.pid")
	pidFile = currentDir + pidFile
	// Read in the pid file as a slice of bytes.
	if piddata, err := ioutil.ReadFile(pidFile); err == nil {
		// Convert the file contents to an integer.
		if pid, err := strconv.Atoi(string(piddata)); err == nil {
			// Look for the pid in the process list.
			if process, err := os.FindProcess(pid); err == nil {
				// Send the process a signal zero kill.
				if err := process.Signal(syscall.Signal(0)); err == nil {
					// We only get an error if the pid isn't running, or it's not ours.
					logger.GetLogger().Log(logger.Alert, fmt.Sprintf("watchdog pid already running: %d", pid))
					return fmt.Errorf("watchdog pid already running: %d", pid)
				}
			}
		}
	}
	// If we get here, then the pidfile didn't exist,
	// or the pid in it doesn't belong to the user running this app.
	return ioutil.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0664)
}
