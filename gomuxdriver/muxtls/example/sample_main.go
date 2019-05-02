package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/paypal/hera/gomuxdriver/muxtls"
	// _ "github.com/paypal/hera/gomuxdriver/muxtls" // if you don't need to poke into muxtls
)

func main() {
	muxtls.OccTLSDrv.TLSCfg.InsecureSkipVerify = true
	// VerifyPeerCertificate func should be set for production

	db, err := sql.Open("occ", "1:127.0.0.1:10101")


	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Println("conn err",err.Error())
		return
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, "select rstatus, wstatus from sample")
	if err != nil {
		fmt.Println("prep err",err.Error())
		return
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		fmt.Println("query err",err.Error())
		return
	}
	defer rows.Close()

	for rows.Next() {
		var rstatus, wstatus sql.NullString
		err = rows.Scan(&rstatus, &wstatus)
		if err != nil {
			fmt.Println("fetch err",err.Error())
			return
		}
	}
}
