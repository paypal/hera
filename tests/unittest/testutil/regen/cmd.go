/*
Copyright 2023 PayPal Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

/*
Regenerates and rewrites code from the "unittest" integration tests to 
go1.20 integration executables
*/

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) <= 1 {
		fmt.Printf("need cmd arg\n")
		os.Exit(3)
	}
	cmd := os.Args[1]
	fmt.Printf("cmd is %s\n",cmd)

	if cmd == "rewrite" {
		if len(os.Args) <= 2 {
			fmt.Printf("need path like tests/unittest/querybindblocker")
			os.Exit(4)
		}
		path := os.Args[2]
		doRewrite(path)
	}
}

func doRewrite(path string) {
	// open path/main_test.go
	// write path/main.go

	f, err := os.Open(os.Getenv("GOPATH")+ "/src/github.com/paypal/hera/"+ path + "/main_test.go")
	if err != nil {
		fmt.Printf("err opening %s/main_test.go %v\n",path, err)
		os.Exit(5)
	}
	defer f.Close()

	// TODO may clobber existing file
	fo, err := os.Create(os.Getenv("GOPATH")+ "/src/github.com/paypal/hera/"+ path + "/main.go")
	if err != nil {
		fmt.Printf("err create %s/main.go %v\n",path, err)
		os.Exit(6)
	}
	defer fo.Close()


	scnln:= bufio.NewScanner(f)
	numln := 0
	fns := make([]string,0)
	beforeFnName := "nil"
	findBeforeFnName := "testutil.UtilMain(m, cfg, "
	for scnln.Scan() {
		numln += 1
		err = scnln.Err()
		if err != nil {
			fmt.Printf("err read line %d %s %v\n", numln, path, err)
			break
		}
		lnstr := scnln.Text()

		/*
		heartbeat doesn't wrap with os.Exit
		unittest/querybindblocker/main_test.go:   os.Exit(testutil.UtilMain(m, cfg, nil))
		unittest/mysql_recycle/main_test.go:      os.Exit(testutil.UtilMain(m, cfg, before))
		unittest/mysql_autocommit/main_test.go:   os.Exit(testutil.UtilMain(m, cfg, setupDb))
		*/
		if strings.Contains(lnstr, findBeforeFnName) {
			_, after, _ := strings.Cut(lnstr, findBeforeFnName)
			lastIdx := len(after)
			if after[lastIdx-2] == ')' {
				lastIdx -= 2
			} else if after[lastIdx-1] == ')' {
				lastIdx -= 1
			}
			beforeFnName = after[:lastIdx]
		}

		if strings.HasPrefix(lnstr, "func Test") && !strings.Contains(lnstr,"TestMain(") {
			before, after, _/*found*/ := strings.Cut(lnstr, "(t *testing.T)")
			fo.WriteString(before + "()" + after + "\n")
			fns = append(fns, before[5:])
		} else if strings.Contains(lnstr, "t.Fatal") {
			// also works for t.Fatalf
			before, after, _ := strings.Cut(lnstr, "t.Fatal")
			fo.WriteString(before + "testutil.Fatal" + after + "\n")
		} else {
			fo.WriteString(lnstr+"\n")
		}
	}
	if err != nil {
		fmt.Printf("inner look break, now exit")
		os.Exit(7)
	}
	fo.WriteString("func main() {\n")
	fo.WriteString("\ttestutil.UtilMain(nil, cfg, "+beforeFnName+")\n")
	for _, fnname := range fns {
		fo.WriteString("\t"+fnname+"()\n")
	}
	fo.WriteString("}\n")
	fmt.Printf("all done %d lines and %d fn's in %s\n",numln,len(fns),path)
}
