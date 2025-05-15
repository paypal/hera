package main

import (
	"fmt"
	"github.com/paypal/hera/tests/util"
	"testing"
)

func TestLiveIssue(t *testing.T) {
	//moveToCutOverPhase(t)
	//util.EnableDebugLog(t)
	//util.RestartOCC(t)
	//os.Setenv("TLS", "1")
	query := "select id FROM db_id_test"
	a, _ := util.CT.ReadQuery(query)
	fmt.Println(a)
}
