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
