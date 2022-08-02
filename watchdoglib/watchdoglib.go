// Copyright 2022 PayPal Inc.

package watchdoglib

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/paypal/hera/utility/logger"
)

type ProcessData struct {
	currPid    int
	startCount int64

	PathToChildExecutable string
	Args                  []string
	err                   error
	processStartFailed    bool
	cmd                   *exec.Cmd
}

type Watchdog struct {
	ReqStopWatchdog chan bool
	Done            chan bool
	pid             int
	err             error

	mut              sync.Mutex
	processList      []*ProcessData
	shutdown         bool
	exitAfterReaping bool
}

/*
 *  It create child process for provided executable path arguments details
 */
func NewChildProcess(pathToChildExecutable string, args ...string) *ProcessData {
	cpOfArgs := make([]string, 0)
	for i := range args {
		cpOfArgs = append(cpOfArgs, args[i])
	}
	fmt.Println(cpOfArgs)

	proc := &ProcessData{
		PathToChildExecutable: pathToChildExecutable,
		Args:                  cpOfArgs,
	}
	return proc
}

/*
 * Iterate over executable list for each executable list it create ProcessData and append ProcessData to a list
 * Will use the list to construct the watchdog instance.
 * This watchdog instance monitors all provided processes in list
 */
func NewWatchdog(executableList [][]string) *Watchdog {

	processList := []*ProcessData{}
	for i := range executableList {
		if len(executableList[i]) == 1 {
			procData := NewChildProcess(executableList[i][0])
			processList = append(processList, procData)
		} else {
			procData := NewChildProcess(executableList[i][0], executableList[i][1:]...)
			processList = append(processList, procData)
		}
	}

	w := &Watchdog{
		processList:     processList,
		ReqStopWatchdog: make(chan bool),
		Done:            make(chan bool),
	}
	return w
}

func (w *Watchdog) AlreadyDone() bool {
	select {
	case <-w.Done:
		return true
	default:
		return false
	}
}
func (w *Watchdog) Stop() error {
	if w.AlreadyDone() {
		// once Done, w.err is immutable, so we don't need to lock.
		return w.err
	}
	w.mut.Lock()
	if w.shutdown {
		defer w.mut.Unlock()
		return w.err
	}
	w.mut.Unlock()

	close(w.ReqStopWatchdog)
	// don't wait for Done while holding the lock,
	// as that is deadlock prone.

	w.mut.Lock()
	defer w.mut.Unlock()
	w.shutdown = true
	return w.err
}

func (w *Watchdog) SetErr(err error) {
	w.mut.Lock()
	defer w.mut.Unlock()
	w.err = err
}

func (w *Watchdog) GetErr() error {
	w.mut.Lock()
	defer w.mut.Unlock()
	return w.err
}

//Start child process
func (processData *ProcessData) startProcess(parentProcessId int) error {
	if processData.cmd != nil {
		processData.cmd.Process.Release()
	}
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("about to start '%s'", processData.PathToChildExecutable))
	processCommand := exec.Command(processData.PathToChildExecutable, processData.Args...)
	processCommand.Stdout = os.Stdout
	err := processCommand.Start()
	if err != nil {
		processData.err = err
		processData.processStartFailed = true
		logger.GetLogger().Log(logger.Alert, fmt.Sprintf("failed to start process '%s' err: %v", processData.PathToChildExecutable, err))
		return err
	}
	processData.cmd = processCommand
	processData.currPid = processData.cmd.Process.Pid
	processData.startCount++
	logger.GetLogger().Log(logger.Alert, fmt.Sprintf("start number %d: watchdog process: %d started new child process '%s' and pid: %d", processData.startCount, parentProcessId, processData.PathToChildExecutable, processData.currPid))
	return nil
}

//Start watchdog process
func (w *Watchdog) Start() {
	processStartFailureCount := 0
	signalChild := make(chan os.Signal, len(w.processList))

	signal.Ignore(syscall.SIGPIPE)
	signal.Notify(signalChild, syscall.SIGCHLD)

	w.pid = os.Getpid()
	//var ws syscall.WaitStatus
	go func() {
		defer func() {
			signal.Stop(signalChild) // reverse the effect of the above Notify()
			if w.processList != nil {
				for index := range w.processList {
					if w.processList[index].cmd != nil {
						logger.GetLogger().Log(logger.Alert, fmt.Sprintf("watchdog releasing child process : %d", w.processList[index].currPid))
						if err3 := w.processList[index].cmd.Process.Kill(); err3 != nil {
							logger.GetLogger().Log(logger.Alert, fmt.Sprintf("watchdog failed to release child process : %d", w.processList[index].currPid))
						}
					}
				}
			}
			close(w.Done)
			// can deadlock if we don't close(w.Done) before grabbing the mutex:
			w.mut.Lock()
			w.shutdown = true
			w.mut.Unlock()
		}()
		//Iterate over processes and start each child-process daemon
		for index := range w.processList {
			err := w.processList[index].startProcess(w.pid)
			if err != nil {
				processStartFailureCount++
			}
		}
		if processStartFailureCount == len(w.processList) {
			logger.GetLogger().Log(logger.Alert, "starting of all child processes failed")
			return
		}
		var ws syscall.WaitStatus
	reaploop:
		for {
			select {
			case <-w.ReqStopWatchdog:
				logger.GetLogger().Log(logger.Info, "request to stop watchdog noted, exiting watchdog.start() loop")
				return
			case <-signalChild:
				logger.GetLogger().Log(logger.Debug, "got signal <-signalChild")
				for i := 0; i < 1000; i++ {
					pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
					// pid > 0 => pid is the ID of the child that died, but
					//  there could be other children that are signalling us
					//  and not the one we in particular are waiting for.
					// pid -1 && errno == ECHILD => no new status children
					// pid -1 && errno != ECHILD => syscall interupped by signal
					// pid == 0 => no more children to wait for.
					logger.GetLogger().Log(logger.Info, fmt.Sprintf(" pid=%v  ws=%v and err == %v", pid, ws, err))
					switch {
					case err != nil:
						err = fmt.Errorf("wait4() got error back: '%s' and ws:%v", err, ws)
						logger.GetLogger().Log(logger.Alert, fmt.Sprintf("warning in reaploop, wait4 returned error: '%s'. ws=%v", err, ws))
						w.SetErr(err)
						continue reaploop
					case pid > 0:
						for index := range w.processList {
							if pid == w.processList[index].currPid {
								logger.GetLogger().Log(logger.Alert, fmt.Sprintf("watchdog saw its child pid: %d, process '%s' finish with waitstatus: %v.", pid, w.processList[index].PathToChildExecutable, ws))
								w.mut.Lock()
								w.processList[index].currPid = 0
								startError := w.processList[index].startProcess(w.pid)
								w.mut.Unlock()
								if startError != nil {
									return
								}
								break
							}
						}
						if w.exitAfterReaping {
							logger.GetLogger().Log(logger.Alert, "watchdog sees exitAfterReaping. exiting now.")
							return
						}
						continue reaploop
					case pid == 0:
						// this is what we get when SIGSTOP is sent on OSX. ws == 0 in this case.
						// Note that on OSX we never get a SIGCONT signal.
						// Under WNOHANG, pid == 0 means there is nobody left to wait for,
						// so just go back to waiting for another SIGCHLD.
						logger.GetLogger().Log(logger.Alert, fmt.Sprintf("pid=0 on wait4, (perhaps SIGSTOP?): nobody left to wait for, keep looping. ws = %v", ws))
						continue reaploop
					default:
						logger.GetLogger().Log(logger.Alert, " warning in reaploop: wait4 negative or not our pid, sleep and try again")
						time.Sleep(time.Millisecond)
					}
				} // end for i
				w.SetErr(fmt.Errorf("could not reap child pid %d or obtain wait4(WNOHANG)==0 even after 1000 attempts", w.pid))
				logger.GetLogger().Log(logger.Alert, fmt.Sprintf("%s", w.err))
				return
			} // end select
		} // end for reaploop
	}()
}
