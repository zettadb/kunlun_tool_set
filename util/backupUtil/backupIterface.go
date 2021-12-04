/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package backupUtil

import (
	"zetta_util/util/configParse"
)

type FileOperateInterface interface {
	PushFileToRemote() error
}

func FileOperatorFactory(tp string) FileOperateInterface {
	return nil
}

type backupInstance interface {
	ColdBackup(arguments *configParse.BackupUtilArguments) error
	IncreamentalLogBackup(arguments *configParse.BackupUtilArguments) error
}

func RunColdBackup(instance backupInstance) error {
	err := instance.ColdBackup(&configParse.BackupUtilArgs)
	if err != nil {
		return err
	}
	return nil
}
func RunIncreamentalLogBackup(instance backupInstance) error {
	err := instance.IncreamentalLogBackup(&configParse.BackupUtilArgs)
	if err != nil {
		return err
	}
	return nil
}

func RunBackup() error {

	mysqlBackup := NewDoMysqlBackupType()
	postgresBackup := &DoPostgresBackupType{}
	var err error
	err = RunColdBackup(mysqlBackup)
	if err != nil {
		return err
	}
	err = RunColdBackup(postgresBackup)
	if err != nil {
		return err
	}

	err = RunIncreamentalLogBackup(mysqlBackup)
	if err != nil {
		return err
	}
	err = RunIncreamentalLogBackup(postgresBackup)
	if err != nil {
		return err
	}
	return nil
}
