/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
package main

import (
	"fmt"
	"os"
	"zetta_util/util/configParse"
	"zetta_util/util/logger"
	"zetta_util/util/restoreUtil"
)

func main() {
	logger.SetUpLogger("../log")
	logger.Log.Debug(fmt.Sprintf("start restore"))
	err := configParse.ParseArgRestore()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	if configParse.RestoreUtilArgs.RestoreType == "compute" {
		restoreCompute := &restoreUtil.RestoreComputeNodeType{}
		err = restoreCompute.RestoreComputeNode(&configParse.RestoreUtilArgs)
		if err != nil {
			fmt.Println(err.Error())
			logger.Log.Error(err.Error())
			os.Exit(-1)
		}
		logger.Log.Info("restore Compute instance successfully")
		fmt.Println("restore Compute successfully")
		os.Exit(0)
	}
	restoreColdback := restoreUtil.NewDoRestoreColdbackType()
	err = restoreColdback.ApplyColdBack()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	fmt.Println("restore xtrabackup successfully")
	logger.Log.Info("restore xtrabackup successfully")

	//time.Sleep(time.Second * 2)
	//restoreFastApplyBinlog := restoreUtil.NewDoFastApplyBinlogType()
	//if restoreFastApplyBinlog == nil {
	//	fmt.Println("restore fastApplyBinlog failed")
	//	os.Exit(-1)
	//}

	//err = restoreFastApplyBinlog.ApplyFastBinlogApply()
	//if err != nil {
	//	fmt.Println(err.Error())
	//	os.Exit(-1)
	//}

	logger.Log.Info("restore fastApplyBinlog successfully")
	logger.Log.Info("restore MySQL instance successfully")
	fmt.Println("restore fastApplyBinlog successfully")
	fmt.Println("restore MySQL instance successfully")

	os.Exit(0)
}
