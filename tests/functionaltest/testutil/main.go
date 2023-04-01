package testutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

type cfgFunc func() (map[string]string, map[string]string, WorkerType)
type beforeFunc func() error

var mx Mux
var localFolder, runFolder string

type clientinfo struct {
	Appname string
	Host    string
}

var source clientinfo

func setup(cfg cfgFunc) error {
	appcfg, opscfg, wType := cfg()
	var err error
	mx, err = NewMux(wType, appcfg, opscfg)
	if err != nil {
		return err
	}

	return mx.StartServer()
}

func GetClientInfo() *clientinfo {
	return &source
}

func teardown() {
	fmt.Println("Tear down tests...")
	mx.StopServer()
}

func copyFile(src, dest string) error {
	content, err := ioutil.ReadFile(src)
	if err == nil {
		err = ioutil.WriteFile(dest, content, 0644)
	}
	return err
}

func saveLogs() {
	fmt.Printf("Saving logs from %s to %s\n", runFolder, localFolder)
	if runFolder != "" {
		err := copyFile(runFolder+"/hera.log", localFolder+"/hera.log")
		if err != nil {
			fmt.Printf("Error saving hera.log from %s to %s: %v\n", runFolder, localFolder, err)
		}
		err = copyFile(runFolder+"/state.log", localFolder+"/state.log")
		if err != nil {
			fmt.Printf("Error saving state.log from %s to %s: %v\n", runFolder, localFolder, err)
		}
		err = copyFile(runFolder+"/cal.log", localFolder+"/cal.log")
		if err != nil {
			fmt.Printf("Error saving cal.log from %s to %s: %v\n", runFolder, localFolder, err)
		}
	}
}

func DoDefaultValidation(t *testing.T) {
	fmt.Println("At DoDefaultValidation")
	c1 := RegexCount("panic")
	if c1 > 0 {
		t.Fatalf("Error: PANIC FOUND !!! PLEASE CHECK!!")
	}
	c2 := RegexCount("signal 11")
	if c2 > 0 {
		t.Fatalf("Error: SIGNAL 11 FOUND !!! PLEASE CHECK!!")
	}
	c3 := RegexCount("unexpected child termination")
	if c3 > 0 {
		t.Fatalf("Error: unexpected child termination! PLEASE CHECK!!")
	}
	c4 := RegexCountFile("ERROR", "cal.log")
	if c4 > 0 {
		if RegexCountFile("no_shard_map", "cal.log") < 1 {
			t.Fatalf("Error: unexpected ERROR in CAL ! PLEASE CHECK!!")
		}
	}

}

func UtilMain(m *testing.M, cfg cfgFunc, before beforeFunc) int {
      	localFolder, _ = os.Getwd()
	err := setup(cfg)
	runFolder, _ = os.Getwd()
	if err != nil {
		fmt.Println("Error setup:", err)
		teardown()
		saveLogs()
		return -1
	}
	if before != nil {
		err = before()
		if err != nil {
			fmt.Println("Error before():", err)
			teardown()
			saveLogs()
			return -1
		}
	}
	code := m.Run()
	teardown()
	if testing.Verbose() {
		saveLogs()
	}
	return code
}
