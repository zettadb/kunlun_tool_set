package restoreUtil

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"zetta_util/util/commonUtil"
	"zetta_util/util/configParse"
	"zetta_util/util/logger"
)

var XtrabackupGtidPos string

type DoRestoreColdbackType struct {
	Param *configParse.RestoreUtilArguments
	Opts  commonUtil.MysqlInstanceOps
}

func NewDoRestoreColdbackType() *DoRestoreColdbackType {
	return &DoRestoreColdbackType{
		Param: &(configParse.RestoreUtilArgs),
		Opts: commonUtil.MysqlInstanceOps{
			MysqlEtcFile: *(configParse.RestoreUtilArgs.MysqlParam),
			BinPath:      "",
			DbaToolPath:  "",
			DirFetched:   false,
		},
	}
}

func (dx *DoRestoreColdbackType) ApplyColdBack() error {

	var err error
	//var retval bool
	err = dx.preCheckInstance()
	if err != nil {
		logger.Debug("%s", err.Error())
		return err
	}
	var xtrabackupBaseDir string
	err, xtrabackupBaseDir = dx.extractColdBackup()
	if err != nil {
		logger.Debug("%s", err.Error())
		return err
	}
	err = dx.parseXtrabackupGtidInfo()
	if err != nil {
		logger.Debug("%s", err.Error())
		return err
	}
	err = dx.doXtrabackupPrepare(xtrabackupBaseDir)
	if err != nil {
		logger.Debug("%s", err.Error())
		return err
	}
	err = dx.doXtrabackupRestore(xtrabackupBaseDir)
	if err != nil {
		logger.Debug("%s", err.Error())
		return err
	}
	err = dx.postOpsAndCheck()
	if err != nil {
		logger.Debug("%s", err.Error())
		return err
	}
	return nil
}

//preCheckInstance focus on the prerequisite of the
//xtrabackup restore.
//
//1. check mysql instance is alived
//2. shut down it (xtrabackup request)
func (dx *DoRestoreColdbackType) preCheckInstance() error {

	//confirm the instance is alive
	err := dx.Opts.IsAlive()
	if err != nil {
		return fmt.Errorf(
			"error info: %s, mysqld is not alive through `ps` by %s",
			err.Error(),
			dx.Param.MysqlParam.Parameters["port"])
	}

	//shut down the dest MySQL instance
	retval, err := dx.Opts.ShutDownByKill()
	if retval == false {
		return err
	}
	return nil
}

func (dx *DoRestoreColdbackType) extractColdBackup() (error, string) {
	_ = os.MkdirAll(configParse.RestoreBaseDir, 0777)
	cmd := fmt.Sprintf("tar xzf %s -C %s", dx.Param.ColdBackupFilePath, configParse.RestoreBaseDir)
	//fmt.Println(cmd)
	sh := commonUtil.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	dir := fmt.Sprintf("%s/xtrabackup_base", configParse.RestoreBaseDir)
	if err != nil {
		return err, dir
	}
	return nil, dir
}

func (dx *DoRestoreColdbackType) parseXtrabackupGtidInfo() error {
	cmd := fmt.Sprintf("cat %s/xtrabackup_base/xtrabackup_binlog_info | cut -f 3", configParse.RestoreBaseDir)
	sh := commonUtil.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}
	gtidinfo := sh.Stdout()
	if len(gtidinfo) == 0 {
		return fmt.Errorf("get invalid gtidinf from xtrabacup_binlog_info file")
	}
	gtidset := strings.Split(gtidinfo, ",")
	for _, gtid := range gtidset {
		tokens := strings.Split(gtid, ":")
		if len(tokens) != 2 {
			return fmt.Errorf("gtidinfo parse failed")
		}
		seqs := tokens[1]
		if strings.Contains(seqs, "-") {
			t := strings.Split(seqs, "-")
			XtrabackupGtidPos = fmt.Sprintf("%s:%s", tokens[0], t[1])
		} else {
			XtrabackupGtidPos = seqs
			XtrabackupGtidPos = fmt.Sprintf("%s:%s", tokens[0], seqs)
		}
	}
	//fmt.Println(XtrabackupGtidPos)
	return nil
}

func (dx *DoRestoreColdbackType) doXtrabackupPrepare(xtrabackupbaseDir string) error {
	args := []string{
		"--prepare",
		fmt.Sprintf("--target-dir=%s", xtrabackupbaseDir),
		"--core-file",
	}
	sh := commonUtil.NewShellRunner("xtrabackup", args)
	err := sh.Run()
	if err != nil {
		return err
	}
	return nil
}

func (dx *DoRestoreColdbackType) doXtrabackupRestore(xtrabackupbaseDir string) error {
	args := []string{
		fmt.Sprintf("--defaults-file=%s", dx.Param.MysqlParam.Path),
		"--copy-back",
		fmt.Sprintf("--target-dir=%s", xtrabackupbaseDir),
		"--core-file",
	}
	//before Doing this, we need to confirm that the dest instance `datadir` is
	//empty, if not ,we will clean it.
	datadir := dx.Param.MysqlParam.Parameters["datadir"]
	err := os.RemoveAll(datadir)
	if err != nil {
		logger.Error("%s", err.Error())
	}
	err = os.Mkdir(datadir, 0755)
	if err != nil {
		logger.Error("%s", err.Error())
	}
	sh := commonUtil.NewShellRunner("xtrabackup", args)
	err = sh.Run()
	if err != nil {
		return err
	}
	return nil

}

func (dx *DoRestoreColdbackType) postOpsAndCheck() error {
	//start mysqld
	err := dx.Opts.StartMysqld()
	if err != nil {
		return err
	}
	err = dx.Opts.IsAlive()
	if err != nil {
		return fmt.Errorf("mysqld is not alive after finish the xtrabackup restore")
	}
	return nil

}

type DoFastApplyBinlogType struct {
	Param             *configParse.RestoreUtilArguments
	ClogObj           *CommitLoggerProcessor
	StartGtid         string
	StartRelayName    string
	StartRelayPos     string
	StopDatetime      string
	RelayLogIndexName string
	BaseDir           string
}

func NewDoFastApplyBinlogType() *DoFastApplyBinlogType {
	pt := &DoFastApplyBinlogType{
		Param:        &(configParse.RestoreUtilArgs),
		StopDatetime: configParse.RestoreUtilArgs.RestoreTime,
		ClogObj:      &CommitLoggerProcessor{},
		BaseDir:      configParse.RestoreBaseDir + "/binlog_base",
		StartGtid:    XtrabackupGtidPos,
	}
	err := pt.Init()
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return pt
}

func (d *DoFastApplyBinlogType) ApplyFastBinlogApply() error {

	var err error
	if d.Param.GlobalConsistentEnable {
		err = d.ClogObj.PrepareCommitLogEntryToFile()
		if err != nil {
			return err
		}
	}
	err = d.ExtractBinlogBackupToRelayPath()
	if err != nil {
		return err
	}
	err = d.GenerateFakeReplicaChannel()
	if err != nil {
		return err
	}
	err = d.StartSlaveSqlThread()
	if err != nil {
		return err
	}
	err = d.CheckSqlThreadState()
	if err != nil {
		return err
	}
	err = d.PostOps()
	if err != nil {
		return err
	}
	return nil
}

func (d *DoFastApplyBinlogType) Init() error {
	if d.Param.GlobalConsistentEnable {
		d.ClogObj.ClogPath = d.BaseDir + "/commit.log"
		d.ClogObj.MetaConnString = configParse.RestoreUtilArgs.MetaClusterConnStr
		d.ClogObj.OrgClusterId = configParse.RestoreUtilArgs.OrigClusterName
		d.ClogObj.RestoreTime = configParse.RestoreUtilArgs.RestoreTime
	}
	_ = os.MkdirAll(d.BaseDir, 0755)
	return nil
}

func (d *DoFastApplyBinlogType) GenerateFakeReplicaChannel() error {

	fp, err := d.GetStartFilePosInfoByGtid()
	if err != nil {
		return err
	}
	d.StartRelayName = fp.name
	d.StartRelayPos = fp.pos
	d.TruncateFileByStoptime()
	var doChangeMasterStmt string
	doChangeMasterStmt = fmt.Sprintf(
		"change master to master_host='2.3.4.5', master_port=7890, master_user='repl',master_password='repl', relay_log_file='%s',relay_log_pos=%s for channel 'fast_apply'",
		d.StartRelayName, d.StartRelayPos,
	)

	fmt.Println(doChangeMasterStmt)

	dbConn, err := configParse.NewMysqlConnectionBySockFile(d.Param.MysqlParam.Parameters["socket"], "root", "root")
	defer func(dbConn *sql.DB) {
		if dbConn != nil {
			dbConn.Close()
		}
	}(dbConn)
	if err != nil {
		return err
	}

	err = dbConn.Ping()
	if err != nil {
		return err
	}

	_, err = dbConn.Query(doChangeMasterStmt)
	if err != nil {
		return err
	}
	return nil
}

func (d *DoFastApplyBinlogType) StartSlaveSqlThread() error {
	var doStartSqlThread string
	doStartSqlThread = fmt.Sprintf("start slave sql_thread for channel 'fast_apply'")

	dbConn, err := configParse.NewMysqlConnectionBySockFile(d.Param.MysqlParam.Parameters["socket"], "root", "root")
	defer func(dbConn *sql.DB) {
		if dbConn != nil {
			dbConn.Close()
		}
	}(dbConn)
	if err != nil {
		return err
	}

	err = dbConn.Ping()
	if err != nil {
		return err
	}
	_, err = dbConn.Query(doStartSqlThread)
	if err != nil {
		return err
	}
	return nil
}

type SlaveStatusInfo struct {
	Slave_IO_State                string
	Master_Host                   string
	Master_User                   string
	Master_Port                   string
	Connect_Retry                 string
	Master_Log_File               string
	Read_Master_Log_Pos           string
	Relay_Log_File                string
	Relay_Log_Pos                 string
	Relay_Master_Log_File         string
	Slave_IO_Running              string
	Slave_SQL_Running             string
	Replicate_Do_DB               string
	Replicate_Ignore_DB           string
	Replicate_Do_Table            string
	Replicate_Ignore_Table        string
	Replicate_Wild_Do_Table       string
	Replicate_Wild_Ignore_Table   string
	Last_Errno                    string
	Last_Error                    string
	Skip_Counter                  string
	Exec_Master_Log_Pos           string
	Relay_Log_Space               string
	Until_Condition               string
	Until_Log_File                string
	Until_Log_Pos                 string
	Master_SSL_Allowed            string
	Master_SSL_CA_File            string
	Master_SSL_CA_Path            string
	Master_SSL_Cert               string
	Master_SSL_Cipher             string
	Master_SSL_Key                string
	Seconds_Behind_Master         string
	Master_SSL_Verify_Server_Cert string
	Last_IO_Errno                 string
	Last_IO_Error                 string
	Last_SQL_Errno                string
	Last_SQL_Error                string
	Replicate_Ignore_Server_Ids   string
	Master_Server_Id              string
	Master_UUID                   string
	Master_Info_File              string
	SQL_Delay                     string
	SQL_Remaining_Delay           string
	Slave_SQL_Running_State       string
	Master_Retry_Count            string
	Master_Bind                   string
	Last_IO_Error_Timestamp       string
	Last_SQL_Error_Timestamp      string
	Master_SSL_Crl                string
	Master_SSL_Crlpath            string
	Retrieved_Gtid_Set            string
	Executed_Gtid_Set             string
	Auto_Position                 string
	Replicate_Rewrite_DB          string
	Channel_Name                  string
	Master_TLS_Version            string
	Master_public_key_path        string
	Get_master_public_key         string
	Network_Namespace             string
}

func (s *SlaveStatusInfo) Ok() bool {

	return true
}

func (s *SlaveStatusInfo) Error() error {
	return nil
}

func (d *DoFastApplyBinlogType) GetSlaveStatusInfoInterval(ch chan SlaveStatusInfo, interval int) {

	showStmt := "show slave status for channel 'fast_apply'"
	dbConn, _ := configParse.NewMysqlConnectionBySockFile(d.Param.MysqlParam.Parameters["socket"], "root", "root")
	if dbConn == nil || dbConn.Ping() != nil {
		return
	}
	for {
		var slaveInfo SlaveStatusInfo
		_ = dbConn.QueryRow(showStmt).Scan(&slaveInfo)
		ch <- slaveInfo
		time.Sleep(time.Duration(interval) * time.Second)
	}

}

func (d *DoFastApplyBinlogType) CheckSqlThreadState() error {

	slaveInfoCh := make(chan SlaveStatusInfo)
	go d.GetSlaveStatusInfoInterval(slaveInfoCh, 2)

	for {
		si := <-slaveInfoCh
		if si.Ok() {
			return nil
		}
		if si.Error() != nil {
			//TODO
			//print something interval
		}
	}
	return nil
}

func (d *DoFastApplyBinlogType) ExtractBinlogBackupToRelayPath() error {
	//Get the relay log path
	para := d.Param.MysqlParam.Parameters["relay-log"]
	relayPath, _ := filepath.Split(para)
	_ = os.MkdirAll(relayPath, 0755)

	//Untar the binlog backup file to the relay path
	cmd := fmt.Sprintf("tar xzf %s -C %s", d.Param.BinlogBackupFilePath, relayPath)
	sh := commonUtil.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}
	orgBinlogFileIndexStr := make(map[int]string)
	//Rename the binlog backup to relay name prefix
	var relayLogfilePath = relayPath + "/binlog_base"
	files, err := ioutil.ReadDir(relayLogfilePath)
	if err != nil {
		return err
	}
	for _, f := range files {
		//fetch index in the whole binlog file name
		fname := f.Name()
		tokens := strings.Split(fname, ".")
		if len(tokens) != 2 {
			return fmt.Errorf("invalid Binlog file name Error %s", fname)
		}
		if tokens[1] == "index" {
			continue
		}
		index, err := strconv.ParseUint(tokens[1], 10, 32)
		if err != nil {
			return err
		}
		orgBinlogFileIndexStr[int(index)] = tokens[1]
	}
	orgIndex := make([]int, 0)
	for kindex, _ := range orgBinlogFileIndexStr {
		orgIndex = append(orgIndex, kindex)
	}
	sort.Slice(orgIndex, func(i, j int) bool {
		return orgIndex[i] < orgIndex[j]
	})

	var relayLogPrefix = "relay-fast_apply."
	relayIndexFileName := relayPath + "/relay-fast_apply.index"
	fdIndex, _ := os.Create(relayIndexFileName)
	d.RelayLogIndexName = relayIndexFileName

	for _, indexInt := range orgIndex {
		newRelayFileName := fmt.Sprintf("%s%s", relayLogPrefix, orgBinlogFileIndexStr[indexInt])
		orgBinlogName := fmt.Sprintf("binlog.%s", orgBinlogFileIndexStr[indexInt])
		err := os.Rename(relayLogfilePath+"/"+orgBinlogName, relayPath+"/"+newRelayFileName)
		if err != nil {
			return err
		}
		absPath := fmt.Sprintf("%s%s", relayPath, newRelayFileName)
		_, err = fdIndex.WriteString(absPath + "\n")
		if err != nil {
			return err
		}
	}
	os.RemoveAll(relayPath + "binlog_base")

	return nil
}
func (d *DoFastApplyBinlogType) PostOps() error {
	var stopSlave, resetSlaveAll string
	stopSlave = fmt.Sprintf("stop slave for channel 'fast_apply';")
	resetSlaveAll = fmt.Sprintf("reset slave all;")

	dbConn, err := configParse.NewMysqlConnectionBySockFile(d.Param.MysqlParam.Parameters["socket"], "root", "root")
	if dbConn == nil {
		return fmt.Errorf("get DB connection by unix Socket faild")
	}

	err = dbConn.Ping()
	if err != nil {
		return err
	}
	_, err = dbConn.Query(stopSlave)
	if err != nil {
		return err
	}
	_, err = dbConn.Query(resetSlaveAll)
	if err != nil {
		return err
	}
	return nil
}

type FilePosInfo struct {
	name string
	pos  string
	gtid string
}

func (f *FilePosInfo) SetGtid(gtid string) {
	f.gtid = gtid
}

func NewFilePosInfo() *FilePosInfo {
	return &FilePosInfo{}
}

func (d *DoFastApplyBinlogType) TruncateFileByStoptime() error {

	var mysqlbinlogUtil string = "mysqlbinlog"
	ops := commonUtil.MysqlInstanceOps{MysqlEtcFile: *(d.Param.MysqlParam)}
	ops.FetchWorkingDir()

	mysqlWorkingDir := ops.BinPath
	cmdname := mysqlWorkingDir + "/" + mysqlbinlogUtil
	args := []string{
		fmt.Sprintf("--binlog-index-file=%s", d.RelayLogIndexName),
		fmt.Sprintf("--stop-datetime='%s'", d.StopDatetime),
		fmt.Sprint("--truncate-file-by-stoptime")}
	sh := commonUtil.NewShellRunner(cmdname, args)
	err := sh.Run()
	if err != nil {
		return err
	}
	return nil

}

func (d *DoFastApplyBinlogType) GetStartFilePosInfoByGtid() (*FilePosInfo, error) {
	filePosInfo := NewFilePosInfo()

	var mysqlbinlogUtil string = "mysqlbinlog"
	ops := commonUtil.MysqlInstanceOps{MysqlEtcFile: *(d.Param.MysqlParam)}
	ops.FetchWorkingDir()
	mysqlWorkingDir := ops.BinPath
	CmdName := mysqlWorkingDir + "/" + mysqlbinlogUtil
	args := []string{
		fmt.Sprintf("--binlog-index-file=%s", d.RelayLogIndexName),
		fmt.Sprintf("--gtid-to-filepos=%s", d.StartGtid)}
	sh := commonUtil.NewShellRunner(CmdName, args)
	err := sh.Run()
	if err != nil {
		return nil, err
	}
	jsonResult := sh.Stdout()
	var result map[string]interface{}
	err = json.Unmarshal([]byte(jsonResult), &result)
	if err != nil {
		return nil, err
	}

	filePosInfo.pos = fmt.Sprintf("%v", result["pos"])
	filePosInfo.name = fmt.Sprintf("%v", result["filename"])
	filePosInfo.gtid = d.StartGtid
	return filePosInfo, nil

}
func (d *DoFastApplyBinlogType) SetStartGtid() {
	//TODO
}

type RestoreObj struct {
	xtrabackuper  DoRestoreColdbackType
	binlogapplyer DoFastApplyBinlogType
}
