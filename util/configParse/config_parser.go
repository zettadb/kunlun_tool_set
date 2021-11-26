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

type tomlConfig struct {
	Title   string
	Restore RestoreUtilArguments
	Backup  BackupUtilArguments
}
type BackupUtilArguments struct {
	MysqlEtcFilePath string
	StorageType      string
	ClusterName      string
}

type RestoreUtilArguments struct {
	ColdBackupFilePath     string
	BinlogBackupFilePath   string
	DefaultFile            string
	GlobalConsistentEnable bool
	RestoreTime            string
	OrigClusterName        string
	MetaClusterConnStr     string
	MysqlParam             *MysqlOptionFile
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
	optionFile.Path = TomlCnf.Restore.DefaultFile
	TomlCnf.Restore.MysqlParam = optionFile
	RestoreUtilArgs = TomlCnf.Restore
	err = RestoreUtilArgs.IsValid()
	if err != nil {
		return err
	}
	initRestoreBaseDir(TomlCnf.Restore.OrigClusterName)
	initBackupBaseDir(TomlCnf.Backup.ClusterName)
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

func initRestoreBaseDir(origClusterName string) {
	Systimestamp := strconv.FormatInt(time.Now().Unix(), 10)
	if len(origClusterName) != 0 {
		RestoreBaseDir = fmt.Sprintf("data/restore-%s-%s", origClusterName, Systimestamp)
	} else {
		RestoreBaseDir = fmt.Sprintf("data/restore-anonymousCluster-%s", Systimestamp)
	}
}

//todo invoke initbackupbasedir
func initBackupBaseDir(clusterName string) {
	Systimestamp := strconv.FormatInt(time.Now().Unix(), 10)
	if len(clusterName) != 0 {
		BackupBaseDir = fmt.Sprintf("data/backup-%s-%s", clusterName, Systimestamp)
	} else {
		BackupBaseDir = fmt.Sprintf("data/backup-anonymousCluster-%s", Systimestamp)
	}
	_ = os.MkdirAll(BackupBaseDir, 0755)
}

func PrintBackupIntro() {
	fmt.Println(Banner)
	intro := `
NAME
	backup - backup kunlun cluster instance 

SYNOPSIS
	backup -config=file.conf
	backup -port=${mysql_listen_port} [-clustername=cname -etcfile=mysql-etc-file -storagetype=hdfs]

DESCRIPTION
	This is the tool to backup the kunlun cluster storage instance (use xtrabackup in default).
	If run successfully ,exit code will be 0 and the tarball of the cold-backup file path will be 
	printed through stdout. Otherwise, exit code will be -1.

CONFIGFILE
	The config file of backup tool should be filed like described below:

	title = "cnf template"

	[backup]
	MysqlEtcFilePath="path"
	StorageType = "hdfs"
	ClusterName = "clust1"

OVERVIEW
`
	fmt.Println(intro)

}

func getEtcFilePathByPort(port string) (string, error) {
	cmd := fmt.Sprintf("ps -ef | grep %s | grep -v grep | grep -v mysqld_safe | awk -F '--defaults-file=' '{print $2}' | awk -F ' ' '{print $1}'", port)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		logger.Log.Error(err.Error())
		return "", err
	}
	logger.Log.Debug(sh.Sh.Bash)
	logger.Log.Debug(fmt.Sprintf("%s", sh.OutPut()))
	return sh.Stdout(), nil

}

func ParseArgBackup() error {

	configFile := flag.String("config", "", "config file, toml")
	port := flag.String("port", "", "the port of mysql which to be backuped")
	etcFile := flag.String("etcfile", "", "path to the etc file of the mysql instance to be backuped")
	storagetype := flag.String("storagetype", "hdfs", "specify the coldback storage type: hdfs ..")
	clustername := flag.String("clustername", "", "name of the cluster to be backuped")
	flag.Parse()

	if len(os.Args) < 2 {
		PrintBackupIntro()
		flag.PrintDefaults()
		return fmt.Errorf("arg parse error")
	}
	if len(*port) != 0 {
		etcpath, err := getEtcFilePathByPort(*port)
		if err != nil {
			logger.Log.Error(err.Error())
			if len(*etcFile) == 0 {
				return err
			}
		}
		*etcFile = etcpath
	}

	if len(*configFile) != 0 {
		err := parseTomlCnf(*configFile)
		if err != nil {
			return err
		}
		return nil
	}
	BackupUtilArgs.MysqlEtcFilePath = *etcFile
	BackupUtilArgs.StorageType = *storagetype
	BackupUtilArgs.ClusterName = *clustername
	initBackupBaseDir(BackupUtilArgs.ClusterName)
	return nil

}
func PrintRestoreIntro() {
	fmt.Println(Banner)
	intro := `
NAME
	restore - restore kunlun cluster instance SN or CN

SYNOPSIS
	restore -config=file.conf
	restore -option=value ... (Listed below the OVERVIEW tag) 

DESCRIPTION
	This is the tool to restore the kunlun cluster storage instance (compute node can be resotred
	by replay the ddl log)
	If run successfully ,exit code will be 0. Otherwise exit code will be -1.

CONFIGFILE
	The config file of restore tool should be filed like described below:

	title = "cnf template"
	[restore]
	ColdBackupFilePath = "/path/to/coldbackup/tarball/base.tgz"
	BinlogBackupFilePath = "/path/to/binlog/backup/file"
	DefaultFile = "/etc/file/of/the/dest/mysql"
	GlobalConsistentEnable = false # Whether enable the global consistent restoration
	RestoreTime = "2021-01-01 11:11:11" # Time stamp to restore

OVERVIEW
`
	fmt.Println(intro)
}

func ParseArgRestore() error {
	optPt := new(MysqlOptionFile)

	configFile := flag.String("config", "", "config file, toml")
	port := flag.String("port", "", "the port of mysql which to be restored")
	coldBackPath := flag.String("backupfile-xtrabackup", "", "path to xtrabackup coldback file")
	binlogBackupPath := flag.String("backupfile-binlog", "", "path to binlog backup")
	etcFile := flag.String("etcfile-new-mysql", "", "path to the etc file of the dest mysql instance")
	consistent := flag.Bool("enable-globalconsistent", false, "whether restore the new mysql under global consistent restrict")
	restoretime := flag.String("restoretime", "", "time point the new mysql restore to")
	origClusterName := flag.String("origclustername", "", "source cluster name to be restored or backuped")
	metaClusterConnStr := flag.String("metaclusterconnstr", "", "meta cluster connection string")

	flag.Parse()

	if len(os.Args) < 2 {
		PrintRestoreIntro()
		flag.PrintDefaults()
		return fmt.Errorf("arg parse error")
	}
	if len(*port) != 0 {
		etcpath, err := getEtcFilePathByPort(*port)
		if err != nil {
			logger.Log.Error(err.Error())
			if len(*etcFile) == 0 {
				return err
			}
		}
		*etcFile = etcpath
	}

	if len(*configFile) != 0 {
		err := parseTomlCnf(*configFile)
		if err != nil {
			return err
		}
		return nil
	}
	initRestoreBaseDir(*origClusterName)

	optPt.Inited = false
	optPt.Parameters = make(map[string]string, 0)
	optPt.Path = *etcFile
	RestoreUtilArgs.MysqlParam = optPt
	RestoreUtilArgs.ColdBackupFilePath = *coldBackPath
	RestoreUtilArgs.BinlogBackupFilePath = *binlogBackupPath
	RestoreUtilArgs.GlobalConsistentEnable = *consistent
	RestoreUtilArgs.RestoreTime = *restoretime
	RestoreUtilArgs.OrigClusterName = *origClusterName
	RestoreUtilArgs.MetaClusterConnStr = *metaClusterConnStr

	err := RestoreUtilArgs.IsValid()
	if err != nil {
		return err
	}
	err = optPt.Parse()
	if err != nil {
		return err
	}

	return nil
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
			m.Parameters[strings.TrimSpace(tokens[0])] = tokens[1]
		} else {
			m.Parameters[tokens[0]] = "SINGLE_KEY"
		}
	}
	return nil
}
