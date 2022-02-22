/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package configParse

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"zetta_util/util/logger"
	"zetta_util/util/shellRunner"
)

var Banner string = `
 ___  __    ___  ___  ________   ___       ___  ___  ________      
|\  \|\  \ |\  \|\  \|\   ___  \|\  \     |\  \|\  \|\   ___  \    
\ \  \/  /|\ \  \\\  \ \  \\ \  \ \  \    \ \  \\\  \ \  \\ \  \   
 \ \   ___  \ \  \\\  \ \  \\ \  \ \  \    \ \  \\\  \ \  \\ \  \  
  \ \  \\ \  \ \  \\\  \ \  \\ \  \ \  \____\ \  \\\  \ \  \\ \  \ 
   \ \__\\ \__\ \_______\ \__\\ \__\ \_______\ \_______\ \__\\ \__\
    \|__| \|__|\|_______|\|__| \|__|\|_______|\|_______|\|__| \|__|

------------------------------------------------------------------------
`
var TomlCnf tomlConfig
var RestoreUtilArgs RestoreUtilArguments
var BackupUtilArgs BackupUtilArguments
var RestoreBaseDir string
var BackupBaseDir string
var HdfsBaseDir = "/kunlun/backup"
var HdfsNameNode string

type tomlConfig struct {
	Title   string
	Restore RestoreUtilArguments
	Backup  BackupUtilArguments
}
type BackupUtilArguments struct {
	BackupType  string
	StorageType string
	ClusterName string
	ShardName   string
	Port        string
	MysqlPara   *MysqlOptionFile
	PgPort      string
	TempDir     string
}

type RestoreUtilArguments struct {
	GlobalConsistentEnable bool
	RestoreTime            string
	OrigClusterName        string
	OrigShardName          string
	MetaClusterConnStr     string
	OrigMetaClusterConnStr string
	TempWorkDir            string
	ColdBackupFilePath     string
	BinlogBackupFilePath   string
	MysqlParam             *MysqlOptionFile
	RestoreType            string
	Port                   string
}

func parseTomlCnf(path string) error {
	fmt.Println(Banner)
	_, err := toml.DecodeFile(path, &TomlCnf)
	if err != nil {
		return err
	}
	optionFile := &MysqlOptionFile{
		Path:       "",
		Parameters: make(map[string]string, 0),
		Inited:     false}
	optionFile.Path = TomlCnf.Restore.MysqlParam.Path
	TomlCnf.Restore.MysqlParam = optionFile
	RestoreUtilArgs = TomlCnf.Restore
	err = RestoreUtilArgs.IsValid()
	if err != nil {
		return err
	}
	initRestoreBaseDir(TomlCnf.Restore.OrigClusterName, TomlCnf.Restore.TempWorkDir)
	initBackupBaseDir(TomlCnf.Backup.ClusterName, TomlCnf.Backup.TempDir)
	err = optionFile.Parse()
	if err != nil {
		return err
	}
	//TODO: backup parameters parse
	BackupUtilArgs = TomlCnf.Backup
	return nil
}

func (u *RestoreUtilArguments) IsValid() error {
	if u.GlobalConsistentEnable {
		if len(u.OrigClusterName) == 0 || len(u.RestoreTime) == 0 || len(u.MetaClusterConnStr) == 0 {
			return fmt.Errorf("if the globalconsistent is true, restoretime, roigclustername, metaclusterconnstr is required")
		}
	}
	return nil

}

func isFlagParsed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func initRestoreBaseDir(origClusterName string, tmpdir string) {
	Systimestamp := strconv.FormatInt(time.Now().Unix(), 10)
	if len(origClusterName) != 0 {
		RestoreBaseDir = fmt.Sprintf("%s/restore-%s-%s", tmpdir, origClusterName, Systimestamp)
	} else {
		RestoreBaseDir = fmt.Sprintf("%s/restore-anonymousCluster-%s", tmpdir, Systimestamp)
	}
	os.MkdirAll(RestoreBaseDir, 0755)
}

//todo invoke initbackupbasedir
func initBackupBaseDir(clusterName string, tempdir string) {
	Systimestamp := strconv.FormatInt(time.Now().Unix(), 10)
	if len(clusterName) != 0 {
		BackupBaseDir = fmt.Sprintf("%s/backup-%s-%s", tempdir, clusterName, Systimestamp)
	} else {
		BackupBaseDir = fmt.Sprintf("%s/backup-anonymousCluster-%s", tempdir, Systimestamp)
	}
	_ = os.MkdirAll(BackupBaseDir, 0755)
}

func PrintBackupIntro() {
	fmt.Println(Banner)
	intro := `
NAME
	backup - backup kunlun cluster instance 

DESCRIPTION
	This is the tool to backup the kunlun cluster storage instance (use xtrabackup in default).
	If run successfully ,exit code will be 0 and the tarball of the cold-backup file path will be 
	printed through stdout. Otherwise, exit code will be -1.

OVERVIEW
`
	fmt.Println(intro)

}

func getMysqlEtcFilePathByPort(port string) (string, error) {
	cmd := fmt.Sprintf("ps -ef | grep %s | grep -v grep | grep -v mysqld_safe | awk -F '--defaults-file=' '{print $2}' | grep -v -e '^$'| awk -F ' ' '{print $1}'", port)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		logger.Log.Error(err.Error())
		return "", err
	}
	logger.Log.Debug(fmt.Sprintf("%s", sh.OutPut()))
	return sh.Stdout(), nil

}

func ParseArgBackup() error {

	backuptype := flag.String("backuptype", "storage", "back up storage node or 'compute' node,default is 'storage'")
	port := flag.String("port", "", "the port of mysql or postgresql instance which to be backuped")
	etcFile := flag.String(
		"etcfile", "",
		`path to the etc file of the mysql instance to be backuped, 
if port is specified and the related instance is running, 
the tool will determine the etcfile path`)
	storagetype := flag.String("coldstoragetype", "hdfs", "specify the coldback storage type: hdfs ..")
	clustername := flag.String("clustername", "", "name of the cluster to be backuped")
	shardname := flag.String("shardname", "", "name of the current shard")
	temdir := flag.String("workdir", "./data", "where store the backup data locally for temp use")
	hdfsnamenode := flag.String("HdfsNameNodeService", "", "specify the hdfs name node service, hdfs://ip:port")

	// TODO :config file is the toml file which includes all the necessary parameters,is not necessary
	// For now , we disable the toml config file option for reason that explict parameter specification is more readable for debug.
	//configFile := flag.String("config", "", "config file, toml")
	flag.Parse()

	//at list one option is specified
	if len(os.Args) < 2 {
		PrintBackupIntro()
		flag.PrintDefaults()
		return fmt.Errorf("arg parse error,at least on args specified")
	}

	HdfsNameNode = *hdfsnamenode
	etcPath, err := getMysqlEtcFileByPortOrProvided(*port, *etcFile)
	if err != nil {
		return err
	}

	//if len(*configFile) != 0 {
	//	err = parseTomlCnf(*configFile)
	//	if err != nil {
	//		return err
	//	}
	//	return nil
	//}
	BackupUtilArgs.StorageType = *storagetype
	BackupUtilArgs.TempDir = *temdir
	BackupUtilArgs.ClusterName = *clustername
	BackupUtilArgs.ShardName = *shardname
	BackupUtilArgs.Port = *port
	if *backuptype == "storage" {
		BackupUtilArgs.MysqlPara = NewMysqlOptionFile(etcPath)
		err = BackupUtilArgs.MysqlPara.Parse()
		if err != nil {
			return err
		}
	}
	initBackupBaseDir(BackupUtilArgs.ClusterName, BackupUtilArgs.TempDir)
	return nil
}
func PrintRestoreIntro() {
	fmt.Println(Banner)
	intro := `
NAME
	restore - restore kunlun cluster instance SN or CN

DESCRIPTION
	This is the tool to restore the kunlun cluster storage instance (compute node can be resotred
	by replay the ddl log)
	If run successfully ,exit code will be 0. Otherwise exit code will be -1.

OVERVIEW
`
	fmt.Println(intro)
}

func ParseArgRestore() error {

	//	configFile := flag.String("config", "", "config file, toml")
	restoreType := flag.String("restoretype", "storage", "restore storage node or 'compute' node,default is 'storage'")
	port := flag.String("port", "", "the port of mysql/postgresql instance which to be restored and needed to be running state")
	defaultfile := flag.String("mysqletcfile", "", "etc file of the mysql which to be restored, if port is provied and mysqld is alive ,no need")
	tmpDir := flag.String("workdir", "./data", "temporary work path to store the coldback or other type files if needed")
	consistent := flag.Bool("enable-globalconsistent", false, "whether restore the new mysql under global consistent restrict")
	restoretime := flag.String("restoretime", "", "time point the new mysql restore to")
	origClusterName := flag.String("origclustername", "", "source cluster name to be restored or backuped")
	origShardName := flag.String("origshardname", "", "source shard name to be restored")
	origMetaClusterConnStr := flag.String("origmetaclusterconnstr", "", "orig meta cluster connection string e.g. user:pass@(ip:port)/mysql")
	metaClusterConnStr := flag.String("metaclusterconnstr", "", "current meta cluster connection string e.g. user:pass@(ip:port)/mysql")
	hdfsnamenode := flag.String("HdfsNameNodeService", "", "specify the hdfs name node service, hdfs://ip:port")

	flag.Parse()

	if len(os.Args) < 2 {
		PrintRestoreIntro()
		flag.PrintDefaults()
		return fmt.Errorf("arg parse error")
	}

	HdfsNameNode = *hdfsnamenode

	var etcFile string
	var err error
	RestoreUtilArgs.GlobalConsistentEnable = *consistent
	RestoreUtilArgs.OrigClusterName = *origClusterName
	RestoreUtilArgs.OrigShardName = *origShardName
	RestoreUtilArgs.RestoreTime = *restoretime
	RestoreUtilArgs.MetaClusterConnStr = *metaClusterConnStr
	RestoreUtilArgs.OrigMetaClusterConnStr = *origMetaClusterConnStr
	RestoreUtilArgs.TempWorkDir = *tmpDir
	RestoreUtilArgs.RestoreType = *restoreType
	RestoreUtilArgs.Port = *port
	if *restoreType == "storage" {
		etcFile, err = getMysqlEtcFileByPortOrProvided(RestoreUtilArgs.Port, *defaultfile)
		if err != nil {
			return err
		}
		RestoreUtilArgs.MysqlParam = NewMysqlOptionFile(etcFile)
		err = RestoreUtilArgs.MysqlParam.Parse()
		if err != nil {
			return err
		}
	}
	err = RestoreUtilArgs.IsValid()
	if err != nil {
		return err
	}

	initRestoreBaseDir(*origClusterName, *tmpDir)
	return nil
}
func getMysqlEtcFileByPortOrProvided(port, defaultFile string) (string, error) {
	if len(port) == 0 && len(defaultFile) == 0 {
		return "", fmt.Errorf("para port or defaultfile not speicified both")
	}

	if len(defaultFile) != 0 {
		return defaultFile, nil
	}
	return getMysqlEtcFilePathByPort(port)
}

func isExists(fileWithPath string) error {
	_, err := os.Stat(fileWithPath)
	if os.IsNotExist(err) {
		return err
	}
	return nil
}

type MysqlOptionFile struct {
	Path       string
	Parameters map[string]string
	Inited     bool
}

func NewMysqlOptionFile(path string) *MysqlOptionFile {
	return &MysqlOptionFile{
		Path:       path,
		Parameters: make(map[string]string, 0),
		Inited:     false}
}

func (m *MysqlOptionFile) Parse() error {
	if m.Inited {
		return nil
	}
	m.Inited = true
	if _, err := os.Stat(m.Path); err != nil {
		return fmt.Errorf("Fatal error config file: %w \n", err)
	}
	fi, err := os.OpenFile(m.Path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("Fatal error config file %w \n", err)
	}
	defer func(fi *os.File) {
		err := fi.Close()
		if err != nil {

		}
	}(fi)

	br := bufio.NewReader(fi)
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		str := string(a)
		if len(a) == 0 ||
			strings.HasPrefix(str, "#") ||
			strings.HasPrefix(str, "//") ||
			strings.HasPrefix(str, "[") ||
			strings.HasPrefix(str, "/") {
			continue
		}
		// valid str
		tokens := strings.SplitN(str, "=", 2)
		if len(tokens) >= 2 {
			m.Parameters[strings.TrimSpace(tokens[0])] = strings.TrimSpace(tokens[1])
		} else {
			m.Parameters[tokens[0]] = "SINGLE_KEY"
		}
	}
	return nil
}
