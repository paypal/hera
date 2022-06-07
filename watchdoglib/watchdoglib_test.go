package watchdoglib

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestListenInterruptSignalKillWatchDog(t *testing.T) {

	duration := 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)

	defer cancel()
	processList := [][]string{{"/bin/sleep", "5"}, {"/Users/rasamala/sleep", "10"}}
	watcher := NewWatchdog(processList)
	watcher.Start()

	// give it a second to get started
	time.Sleep(2 * time.Millisecond)

	// current Process ID is available here; if 0 then
	// it is between restarts and you should poll again.
	pid := watcher.pid
	fmt.Println(pid)
	// ready to stop both child and watchdog
	// watcher.TermChildAndStopWatchdog <- true

	select {
	case <-watcher.Done:
		fmt.Print("watchdog and child pid shutdown.\n")
	case <-ctx.Done():
		fmt.Println("Timed out program.")

	}
}

func TestTermChildAndStopWatchDog(t *testing.T) {

	duration := 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)

	defer cancel()
	processList := [][]string{{"/bin/sleep", "50"}, {"/bin/sleep", "150"}}
	watcher := NewWatchdog(processList)
	watcher.Start()

	// give it a second to get started
	time.Sleep(2 * time.Millisecond)

	// current Process ID is available here; if 0 then
	// it is between restarts and you should poll again.
	pid := watcher.pid
	fmt.Printf("Started process id: %d", pid)
	// ready to stop both child and watchdog
	watcher.ReqStopWatchdog <- true

	select {
	case <-watcher.Done:
		fmt.Print("watchdog and child pid shutdown.\n")
	case <-ctx.Done():
		fmt.Println("Timed out program.")
	}
}

func TestWatchDogChildProcess(t *testing.T) {

	duration := 120 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)

	defer cancel()
	processList := [][]string{{"/bin/sleep", "50"}, {"/bin/sleep", "150"}}
	watcher := NewWatchdog(processList)
	watcher.Start()

	// give it a second to get started
	time.Sleep(2 * time.Millisecond)

	// current Process ID is available here; if 0 then
	// it is between restarts and you should poll again.
	pid := watcher.pid
	fmt.Printf("Watchdog start child process at: %d\n", pid)
	// ready to stop both child and watchdog
	// watcher.TermChildAndStopWatchdog <- true

	select {
	case <-watcher.Done:
		fmt.Print("watchdog and child pid shutdown.\n")
	case <-ctx.Done():
		fmt.Println("Timed out program.")
	}
}
