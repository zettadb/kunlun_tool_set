/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package backupUtil

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"zetta_util/util/configParse"
	"zetta_util/util/logger"
	"zetta_util/util/shellRunner"
)

type DoMysqlBackupType struct {
}

func NewDoMysqlBackupType() *DoMysqlBackupType {
	obj := &DoMysqlBackupType{}
	return obj
}

func (d *DoMysqlBackupType) ColdBackup(arguments *configParse.BackupUtilArguments) error {

	//d.FileOperator.PushFileToRemote()
	cnfile := arguments.MysqlPara
	datadir := cnfile.Parameters["datadir"]
	if len(datadir) == 0 {
		return fmt.Errorf("can't know the datadir of the MySQL instance")
	}
	cmd := fmt.Sprintf("xtrabackup")
	cmdArgs := []string{
		fmt.Sprintf("--defaults-file=%s", arguments.MysqlPara.Path),
		"--no-server-version-check",
		fmt.Sprintf("--backup --target-dir=%s/xtrabackup_base --user=root --password=root > %s/xtrabackup.log 2>&1 ",
			configParse.BackupBaseDir, configParse.BackupBaseDir)}
	sh := shellRunner.NewShellRunner(cmd, cmdArgs)
	err := sh.Run()
	if err != nil {
		logger.Log.Error(err.Error())
		return err
	}

	cmd1 := fmt.Sprintf("cd %s;tar czf coldback.tgz xtrabackup_base", configParse.BackupBaseDir)
	sh1 := shellRunner.NewShellRunner(cmd1, make([]string, 0))
	err = sh1.Run()
	if err != nil {
		logger.Log.Error(err.Error())
		return err
	}
	coldTarball := fmt.Sprintf("%s/coldback.tgz", configParse.BackupBaseDir)
	abspath, _ := filepath.Abs(coldTarball)

	bind_ip := strings.Trim(arguments.MysqlPara.Parameters["bind_address"], " \n")
	bind_ip = strings.Replace(bind_ip, ".", "#", 3)

	machineInfo := fmt.Sprintf("I%s_P%s", bind_ip, arguments.Port)
	// transfer to hdfs
	hdfsRemoteFilename := fmt.Sprintf("%s/xtrabackup/%s/%s/_xtrabackup_coldfile_%s_%s_.tgz",
		configParse.HdfsBaseDir,
		arguments.ClusterName,
		arguments.ShardName,
		machineInfo,
		time.Now().Format("D2006#01#02_T15#04#05"))
	var hdfsColdbackCmd = ""
	if len(configParse.HdfsNameNode) != 0 {
		hdfsColdbackCmd = fmt.Sprintf("hadoop fs -fs %s -appendToFile %s %s", configParse.HdfsNameNode, abspath, hdfsRemoteFilename)
	} else {
		hdfsColdbackCmd = fmt.Sprintf("hadoop fs -appendToFile %s %s", abspath, hdfsRemoteFilename)
	}
	sh2 := shellRunner.NewShellRunner(hdfsColdbackCmd, make([]string, 0))
	err = sh2.Run()
	if err != nil {
		logger.Log.Error(err.Error())
		return err
	}
	logger.Log.Debug(fmt.Sprintf("backup successfully and finished, file path is %s", abspath))

	fmt.Println(abspath)
	fmt.Println(hdfsRemoteFilename)
	return nil

}
func (d *DoMysqlBackupType) IncreamentalLogBackup(arguments *configParse.BackupUtilArguments) error {
	return nil
}
