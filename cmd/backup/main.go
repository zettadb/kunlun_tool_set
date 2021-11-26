package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"zetta_util/util/backupUtil"
	"zetta_util/util/configParse"
	"zetta_util/util/logger"
)

func main() {
	logger.Log.Debug("start backup")
	err := configParse.ParseArgBackup()
	if err != nil {
		logger.Log.Error(err.Error())
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	err = backupUtil.RunBackup()
	if err != nil {
		logger.Log.Error(err.Error())
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	return
}
