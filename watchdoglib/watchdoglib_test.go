package watchdoglib

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"
)

func TestListenInterruptSignalKillWatchDog(t *testing.T) {

	duration := 25 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)

	defer cancel()
	processData := []string{"/bin/sleep", "5"}
	watcher := NewWatchdog(processData)
	watcher.Start()

	// give it a second to get started
	time.Sleep(2 * time.Millisecond)

	// current Process ID is available here; if 0 then
	// it is between restarts and you should poll again.
	pid := watcher.pid
	childPid := watcher.processData.currPid
	fmt.Println("Watchdog process: ", pid)
	fmt.Println("Initial Watchdog Child Process Id: ", childPid)
	// ready to stop both child and watchdog
	// watcher.TermChildAndStopWatchdog <- true

	select {
	case <-watcher.Done:
		fmt.Print("watchdog and child pid shutdown.\n")
	case <-ctx.Done():
		fmt.Println("Timed out program.")

	}
}

func TestTermChildAndVerifyChildProcessState(t *testing.T) {

	duration := 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)

	defer cancel()
	processData := []string{"/bin/sleep", "200"}
	watcher := NewWatchdog(processData)
	watcher.Start()

	// give it a second to get started
	time.Sleep(2 * time.Millisecond)

	// current Process ID is available here; if 0 then
	// it is between restarts and you should poll again.
	pid := watcher.pid
	childPid := watcher.processData.currPid
	fmt.Println("Started Watchdog process: ", pid)
	fmt.Println("Watchdog Child Process Id: ", childPid)
	// ready to stop both child and watchdog
	watcher.ReqStopWatchdog <- true

	select {
	case <-watcher.Done:
		fmt.Print("watchdog and child pid shutdown.\n")
	case <-ctx.Done():
		fmt.Println("Timed out program.")
	}
	// give it a second to get started
	time.Sleep(5 * time.Second)

	if watcher.processData.cmd != nil {
		fmt.Printf("Child process: %d should exist once watcher completes \n", childPid)
		t.Errorf("Child process: %d should exist once watcher completes", childPid)
	}
}

func TestWatchDogChildProcessAndPgId(t *testing.T) {

	duration := 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)

	defer cancel()
	processData := []string{"/bin/sleep", "50"}
	watcher := NewWatchdog(processData)
	watcher.Start()

	// give it a second to get started
	time.Sleep(20 * time.Millisecond)

	// current Process ID is available here; if 0 then
	// it is between restarts and you should poll again.
	pid := watcher.pid
	childPid := watcher.processData.currPid
	fmt.Println("Started Watchdog process: ", pid)
	fmt.Println("Watchdog Child Process Id: ", childPid)

	pgOfChildProcess, err := syscall.Getpgid(childPid)

	if err != nil {
		t.Errorf("Failed to fetch process group Id of child process: %d", childPid)
	}
	fmt.Println("Child process groupId: ", pgOfChildProcess)
	if pgOfChildProcess != childPid {
		fmt.Printf("pgid: %d of child process should be same as child pid: %d \n", pgOfChildProcess, childPid)
		t.Errorf("pgid: %d of child process should be same as child pid: %d", pgOfChildProcess, childPid)
	}
	// ready to stop both child and watchdog
	watcher.ReqStopWatchdog <- true

	select {
	case <-watcher.Done:
		fmt.Print("watchdog and child pid shutdown.\n")
	case <-ctx.Done():
		fmt.Println("Timed out program.")
	}
}
