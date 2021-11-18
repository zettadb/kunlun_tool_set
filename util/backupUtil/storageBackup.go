package backupUtil

import (
	"fmt"
	"path/filepath"
	"zetta_util/util/commonUtil"
	"zetta_util/util/configParse"
)

type DoMysqlBackupType struct {
	FileOperator FileOperateInterface
}

func NewDoMysqlBackupType() *DoMysqlBackupType {
	obj := &DoMysqlBackupType{}
	obj.FileOperator = FileOperatorFactory(configParse.BackupUtilArgs.StorageType)
	return obj
}

func (d *DoMysqlBackupType) ColdBackup(arguments *configParse.BackupUtilArguments) error {

	//d.FileOperator.PushFileToRemote()
	//parse the mysql etc file
	cnfile := configParse.MysqlOptionFile{
		Path:       arguments.MysqlEtcFilePath,
		Parameters: make(map[string]string),
		Inited:     false}

	err := cnfile.Parse()
	if err != nil {
		return err
	}
	datadir := cnfile.Parameters["datadir"]
	if len(datadir) == 0 {
		return fmt.Errorf("can't know the datadir of the MySQL instance")
	}
	cmd := fmt.Sprintf("xtrabackup")
	cmdArgs := []string{
		fmt.Sprintf("--defaults-file=%s", arguments.MysqlEtcFilePath),
		"--no-server-version-check",
		fmt.Sprintf("--backup --target-dir=%s/xtrabackup_base --user=root --password=root > %s/xtrabackup.log 2>&1 ",
			configParse.BackupBaseDir, configParse.BackupBaseDir)}
	sh := commonUtil.NewShellRunner(cmd, cmdArgs)
	err = sh.Run()
	if err != nil {
		return err
	}

	cmd1 := fmt.Sprintf("cd %s;tar czf coldback.tgz xtrabackup_base", configParse.BackupBaseDir)
	sh1 := commonUtil.NewShellRunner(cmd1, make([]string, 0))
	err = sh1.Run()
	if err != nil {
		return err
	}
	coldTarball := fmt.Sprintf("%s/coldback.tgz", configParse.BackupBaseDir)
	abspath, _ := filepath.Abs(coldTarball)
	fmt.Println(abspath)

	return nil

}
func (d *DoMysqlBackupType) IncreamentalLogBackup(arguments *configParse.BackupUtilArguments) error {
	return nil
}
