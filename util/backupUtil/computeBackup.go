/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package backupUtil

import "zetta_util/util/configParse"

type DoPostgresBackupType struct {
}

func (d *DoPostgresBackupType) ColdBackup(arguments *configParse.BackupUtilArguments) error {
	return nil
}

func (d *DoPostgresBackupType) IncreamentalLogBackup(arguments *configParse.BackupUtilArguments) error {
	return nil
}
