package main

import (
	"fmt"
	"os"
	"time"
	"zetta_util/util/configParse"
	"zetta_util/util/restoreUtil"
)

func main() {

	err := configParse.ParseArgRestore()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	restoreColdback := restoreUtil.NewDoRestoreColdbackType()
	err = restoreColdback.ApplyColdBack()
	//_ = restoreUtil.NewDoRestoreColdbackType()
	//restoreUtil.XtrabackupGtidPos = "3ea32e42-45dd-11ec-be28-7c8ae18d3c61:174"
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
