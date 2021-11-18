package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"zetta_util/util/backupUtil"
	"zetta_util/util/configParse"
)

func main() {
	err := configParse.ParseArgBackup()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	err = backupUtil.RunBackup()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	return
}
