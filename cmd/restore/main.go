package main

import (
	"fmt"
	"os"
	"time"
	"zetta_util/util/configParse"
	"zetta_util/util/logger"
	"zetta_util/util/restoreUtil"
)

func main() {
	logger.Log.Debug(fmt.Sprintf("start restore"))
	err := configParse.ParseArgRestore()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	restoreColdback := restoreUtil.NewDoRestoreColdbackType()
	err = restoreColdback.ApplyColdBack()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	fmt.Println("restore xtrabackup successfully")

	time.Sleep(time.Second * 2)
	restoreFastApplyBinlog := restoreUtil.NewDoFastApplyBinlogType()
	if restoreFastApplyBinlog == nil {
		fmt.Println("restore fastApplyBinlog failed")
		os.Exit(-1)
	}

	err = restoreFastApplyBinlog.ApplyFastBinlogApply()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	fmt.Println("restore fastApplyBinlog successfully")
	fmt.Println("restore MySQL instance successfully")

	os.Exit(0)
}
