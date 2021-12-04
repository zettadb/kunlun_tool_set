/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package restoreUtil

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/metakeule/fmtdate"
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
	"zetta_util/util/shellRunner"
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
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
		return err
	}
	var xtrabackupBaseDir string
	err, xtrabackupBaseDir = dx.extractColdBackup()
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
		return err
	}
	err = dx.parseXtrabackupGtidInfo()
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
		return err
	}
	err = dx.doXtrabackupPrepare(xtrabackupBaseDir)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
		return err
	}
	err = dx.doXtrabackupRestore(xtrabackupBaseDir)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
		return err
	}
	err = dx.postOpsAndCheck()
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
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
		logger.Log.Error(err.Error())
		return err
	}
	//shut down the dest MySQL instance
	retval, err := dx.Opts.ShutDownByKill()
	if retval == false {
		return err
	}
	return nil
}

func (dx *DoRestoreColdbackType) DownloadColdXtraFileByTime(storePath *string, remotePath string, timePoint string) error {

	cmd := fmt.Sprintf("hadoop fs -ls %s", remotePath)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}
	lsOutPut := sh.Stdout()
	timePathMap := make(map[int64]string, 0)
	for _, lines := range strings.Split(lsOutPut, "\n") {
		if strings.HasSuffix(lines, ".tgz") {
			tokenVec := strings.Split(lines, "_")
			vecSize := len(tokenVec)
			timeV := tokenVec[vecSize-3 : vecSize-1]

			year := strings.TrimPrefix(timeV[0], "D")
			times := strings.TrimPrefix(timeV[1], "T")
			unixTime, err := fmtdate.Parse("YYYY#MM#DD_hh#mm#ss",
				fmt.Sprintf("%s_%s", year, times))
			if err != nil {
				fmt.Println(err.Error())
			}

			lineV := strings.Split(lines, " ")
			line := lineV[len(lineV)-1:][0]
			timePathMap[unixTime.Unix()] = line
		}
	}
	var restoreTimeUnix int64
	restoreTime, err := fmtdate.Parse("YYYY-MM-DD hh:mm:ss", timePoint)
	if err != nil {
		return err
	}
	restoreTimeUnix = restoreTime.Unix()
	sortedKey := make([]int64, 0)
	for key, _ := range timePathMap {
		sortedKey = append(sortedKey, key)
	}
	//do sort
	sort.Slice(sortedKey, func(i, j int) bool {
		return sortedKey[i] <= sortedKey[j]
	})

	var index int = 0
	var key int64 = 0
	for index, key = range sortedKey {
		if key >= restoreTimeUnix {
			break
		}
	}
	if index > 0 {
		index = index - 1
	}

	key = sortedKey[index]
	if restoreTimeUnix-key >= 86400 {
		return fmt.Errorf("latest cold back file is %s, some cold back may lost", timePathMap[key])
	}
	xtrabacupFullPathLocal := fmt.Sprintf("%s/xtraColdFile.tgz", *storePath)
	dx.Param.ColdBackupFilePath = xtrabacupFullPathLocal

	cmd = fmt.Sprintf("hadoop fs -get %s %s", timePathMap[key], xtrabacupFullPathLocal)
	fmt.Println(cmd)
	sh1 := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err = sh1.Run()
	if err != nil {
		return err
	}
	return nil
}

func (dx *DoRestoreColdbackType) fetchColdBackTarBall() error {
	path := configParse.RestoreBaseDir
	clusterName := dx.Param.OrigClusterName
	shardName := dx.Param.OrigShardName

	hdfsPath := fmt.Sprintf("%s/xtrabackup/%s/%s", configParse.HdfsBaseDir, clusterName, shardName)

	err := dx.DownloadColdXtraFileByTime(&path, hdfsPath, dx.Param.RestoreTime)
	if err != nil {
		return err
	}
	return nil

}

func (dx *DoRestoreColdbackType) extractColdBackup() (error, string) {
	err := dx.fetchColdBackTarBall()
	if err != nil {
		return err, ""
	}
	_ = os.MkdirAll(configParse.RestoreBaseDir, 0777)
	cmd := fmt.Sprintf("tar xzf %s -C %s", dx.Param.ColdBackupFilePath, configParse.RestoreBaseDir)
	//fmt.Println(cmd)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err = sh.Run()
	dir := fmt.Sprintf("%s/xtrabackup_base", configParse.RestoreBaseDir)
	if err != nil {
		return err, dir
	}
	return nil, dir
}

func (dx *DoRestoreColdbackType) parseXtrabackupGtidInfo() error {
	cmd := fmt.Sprintf("cat %s/xtrabackup_base/xtrabackup_binlog_info | cut -f 3", configParse.RestoreBaseDir)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
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
	sh := shellRunner.NewShellRunner("xtrabackup", args)
	err := sh.Run()
	if err != nil {
		return err
	}
	return nil
}

func (dx *DoRestoreColdbackType) doXtrabackupRestore(xtrabackupbaseDir string) error {
	//before Doing this, we need to confirm that the dest instance `datadir` is
	//empty, if not ,we will clean it.
	datadir := dx.Param.MysqlParam.Parameters["datadir"]
	binlogdir := filepath.Dir(dx.Param.MysqlParam.Parameters["log-bin"])
	relaydir := filepath.Dir(dx.Param.MysqlParam.Parameters["relay-log"])
	err := os.RemoveAll(datadir)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
	}
	err = os.RemoveAll(binlogdir)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
	}
	err = os.RemoveAll(relaydir)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
	}

	err = os.MkdirAll(datadir, 0755)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
	}
	err = os.MkdirAll(binlogdir, 0755)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
	}
	err = os.MkdirAll(relaydir, 0755)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("%s", err.Error()))
	}
	args := []string{
		fmt.Sprintf("--defaults-file=%s", dx.Param.MysqlParam.Path),
		"--copy-back",
		fmt.Sprintf("--target-dir=%s", xtrabackupbaseDir),
		"--core-file",
	}
	sh := shellRunner.NewShellRunner("xtrabackup", args)
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
	err = d.fetchBinlogFileFromHdfs()
	if err != nil {
		return err
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

	logger.Log.Debug("Will do: %s", doChangeMasterStmt)

	dbConn, err := configParse.NewMysqlConnectionBySockFile(d.Param.MysqlParam.Parameters["socket"], "root", "root")
	defer func(dbConn *sqlx.DB) {
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
	defer func(dbConn *sqlx.DB) {
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
	Slave_IO_State                sql.NullString
	Master_Host                   sql.NullString
	Master_User                   sql.NullString
	Master_Port                   sql.NullString
	Connect_Retry                 sql.NullString
	Master_Log_File               sql.NullString
	Read_Master_Log_Pos           sql.NullString
	Relay_Log_File                sql.NullString
	Relay_Log_Pos                 sql.NullString
	Relay_Master_Log_File         sql.NullString
	Slave_IO_Running              sql.NullString
	Slave_SQL_Running             sql.NullString
	Replicate_Do_DB               sql.NullString
	Replicate_Ignore_DB           sql.NullString
	Replicate_Do_Table            sql.NullString
	Replicate_Ignore_Table        sql.NullString
	Replicate_Wild_Do_Table       sql.NullString
	Replicate_Wild_Ignore_Table   sql.NullString
	Last_Errno                    sql.NullString
	Last_Error                    sql.NullString
	Skip_Counter                  sql.NullString
	Exec_Master_Log_Pos           sql.NullString
	Relay_Log_Space               sql.NullString
	Until_Condition               sql.NullString
	Until_Log_File                sql.NullString
	Until_Log_Pos                 sql.NullString
	Master_SSL_Allowed            sql.NullString
	Master_SSL_CA_File            sql.NullString
	Master_SSL_CA_Path            sql.NullString
	Master_SSL_Cert               sql.NullString
	Master_SSL_Cipher             sql.NullString
	Master_SSL_Key                sql.NullString
	Seconds_Behind_Master         sql.NullString
	Master_SSL_Verify_Server_Cert sql.NullString
	Last_IO_Errno                 sql.NullString
	Last_IO_Error                 sql.NullString
	Last_SQL_Errno                sql.NullString
	Last_SQL_Error                sql.NullString
	Replicate_Ignore_Server_Ids   sql.NullString
	Master_Server_Id              sql.NullString
	Master_UUID                   sql.NullString
	Master_Info_File              sql.NullString
	SQL_Delay                     sql.NullString
	SQL_Remaining_Delay           sql.NullString
	Slave_SQL_Running_State       sql.NullString
	Master_Retry_Count            sql.NullString
	Master_Bind                   sql.NullString
	Last_IO_Error_Timestamp       sql.NullString
	Last_SQL_Error_Timestamp      sql.NullString
	Master_SSL_Crl                sql.NullString
	Master_SSL_Crlpath            sql.NullString
	Retrieved_Gtid_Set            sql.NullString
	Executed_Gtid_Set             sql.NullString
	Auto_Position                 sql.NullString
	Replicate_Rewrite_DB          sql.NullString
	Channel_Name                  sql.NullString
	Master_TLS_Version            sql.NullString
	Master_public_key_path        sql.NullString
	Get_master_public_key         sql.NullString
	Network_Namespace             sql.NullString
}

func (s *SlaveStatusInfo) Ok() bool {

	if s.Slave_SQL_Running_State.String == "Slave has read all relay log; waiting for more updates" &&
		s.Slave_SQL_Running.String == "Yes" {
		return true
	}
	return false
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
		slaveInfo := SlaveStatusInfo{}
		rows, err := dbConn.Queryx(showStmt)
		if err != nil {
			fmt.Println(err.Error())
		}
		for rows.Next() {
			err := rows.Scan(
				&slaveInfo.Slave_IO_State,
				&slaveInfo.Master_Host,
				&slaveInfo.Master_User,
				&slaveInfo.Master_Port,
				&slaveInfo.Connect_Retry,
				&slaveInfo.Master_Log_File,
				&slaveInfo.Read_Master_Log_Pos,
				&slaveInfo.Relay_Log_File,
				&slaveInfo.Relay_Log_Pos,
				&slaveInfo.Relay_Master_Log_File,
				&slaveInfo.Slave_IO_Running,
				&slaveInfo.Slave_SQL_Running,
				&slaveInfo.Replicate_Do_DB,
				&slaveInfo.Replicate_Ignore_DB,
				&slaveInfo.Replicate_Do_Table,
				&slaveInfo.Replicate_Ignore_Table,
				&slaveInfo.Replicate_Wild_Do_Table,
				&slaveInfo.Replicate_Wild_Ignore_Table,
				&slaveInfo.Last_Errno,
				&slaveInfo.Last_Error,
				&slaveInfo.Skip_Counter,
				&slaveInfo.Exec_Master_Log_Pos,
				&slaveInfo.Relay_Log_Space,
				&slaveInfo.Until_Condition,
				&slaveInfo.Until_Log_File,
				&slaveInfo.Until_Log_Pos,
				&slaveInfo.Master_SSL_Allowed,
				&slaveInfo.Master_SSL_CA_File,
				&slaveInfo.Master_SSL_CA_Path,
				&slaveInfo.Master_SSL_Cert,
				&slaveInfo.Master_SSL_Cipher,
				&slaveInfo.Master_SSL_Key,
				&slaveInfo.Seconds_Behind_Master,
				&slaveInfo.Master_SSL_Verify_Server_Cert,
				&slaveInfo.Last_IO_Errno,
				&slaveInfo.Last_IO_Error,
				&slaveInfo.Last_SQL_Errno,
				&slaveInfo.Last_SQL_Error,
				&slaveInfo.Replicate_Ignore_Server_Ids,
				&slaveInfo.Master_Server_Id,
				&slaveInfo.Master_UUID,
				&slaveInfo.Master_Info_File,
				&slaveInfo.SQL_Delay,
				&slaveInfo.SQL_Remaining_Delay,
				&slaveInfo.Slave_SQL_Running_State,
				&slaveInfo.Master_Retry_Count,
				&slaveInfo.Master_Bind,
				&slaveInfo.Last_IO_Error_Timestamp,
				&slaveInfo.Last_SQL_Error_Timestamp,
				&slaveInfo.Master_SSL_Crl,
				&slaveInfo.Master_SSL_Crlpath,
				&slaveInfo.Retrieved_Gtid_Set,
				&slaveInfo.Executed_Gtid_Set,
				&slaveInfo.Auto_Position,
				&slaveInfo.Replicate_Rewrite_DB,
				&slaveInfo.Channel_Name,
				&slaveInfo.Master_TLS_Version,
				&slaveInfo.Master_public_key_path,
				&slaveInfo.Get_master_public_key,
				&slaveInfo.Network_Namespace,
			)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
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

func (d *DoFastApplyBinlogType) fetchBinlogFileFromHdfs() error {

	binlog_download_base := configParse.RestoreBaseDir + "/binlog_base"
	os.MkdirAll(binlog_download_base, 0755)

	restoreTime, err := fmtdate.Parse("YYYY-MM-DD hh:mm:ss", d.Param.RestoreTime)
	if err != nil {
		return err
	}
	year, month, day := restoreTime.Date()
	date := fmt.Sprintf("D%04d#%02d#%02d", year, month, day)
	hdfsPathPrefix := fmt.Sprintf("%s/binlog/%s/%s/%s",
		configParse.HdfsBaseDir, d.Param.OrigClusterName, d.Param.OrigShardName, date)

	cmd := fmt.Sprintf("hadoop fs -ls %s", hdfsPathPrefix)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err = sh.Run()
	if err != nil {
		return err
	}
	timePathMap := make(map[int64]string, 0)
	splitVec := strings.Split(sh.Stdout(), "\n")
	for _, line := range splitVec {
		if strings.HasSuffix(line, ".lz4") {
			tokenVec := strings.Split(line, "_")
			vecSize := len(tokenVec)
			timeV := tokenVec[vecSize-3 : vecSize-1]

			year := strings.TrimPrefix(timeV[0], "D")
			times := strings.TrimPrefix(timeV[1], "T")
			unixTime, err := fmtdate.Parse("YYYY#MM#DD_hh#mm#ss",
				fmt.Sprintf("%s_%s", year, times))
			if err != nil {
				fmt.Println(err.Error())
			}

			lineV := strings.Split(line, " ")
			line := lineV[len(lineV)-1:][0]
			timePathMap[unixTime.Unix()] = line
		}
	}
	sortedKey := make([]int64, 0)
	for key, _ := range timePathMap {
		sortedKey = append(sortedKey, key)
	}
	//do sort
	sort.Slice(sortedKey, func(i, j int) bool {
		return sortedKey[i] <= sortedKey[j]
	})
	var index int = 0
	var key int64 = 0
	restoreTimeUnix := restoreTime.Unix()
	for index, key = range sortedKey {
		if key > restoreTimeUnix {
			break
		}
	}

	for it := 0; it <= index; it = it + 1 {
		key_ := sortedKey[it]
		binlogPath := timePathMap[key_]
		binlogOrigName := d.fetchBinlogNameFromHdfsPath(binlogPath)
		// download file to the binlog_base
		cmd := fmt.Sprintf("hadoop fs -get %s %s", binlogPath,
			fmt.Sprintf("%s/%s.lz4", binlog_download_base, binlogOrigName))
		sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
		err = sh.Run()
		if err != nil {
			return err
		}

		//decode lz4 file
		cmd = fmt.Sprintf("lz4 -f -d %s %s",
			fmt.Sprintf("%s/%s.lz4", binlog_download_base, binlogOrigName),
			fmt.Sprintf("%s/%s", binlog_download_base, binlogOrigName))
		sh1 := shellRunner.NewShellRunner(cmd, make([]string, 0))
		err = sh1.Run()
		if err != nil {
			return err
		}
		// remove *.lz4 file
		os.RemoveAll(fmt.Sprintf("%s/%s.lz4", binlog_download_base, binlogOrigName))
	}

	d.Param.BinlogBackupFilePath = binlog_download_base

	return nil
}
func (d *DoFastApplyBinlogType) fetchBinlogNameFromHdfsPath(path string) string {
	tokenVec := strings.Split(path, "_")
	for _, words := range tokenVec {
		if strings.HasPrefix(words, "binlog.") {
			return words
		}
	}
	return ""
}

func (d *DoFastApplyBinlogType) ExtractBinlogBackupToRelayPath() error {
	//Get the relay log path
	para := d.Param.MysqlParam.Parameters["relay-log"]
	relayPath, _ := filepath.Split(para)
	_ = os.MkdirAll(relayPath, 0755)

	//Untar the binlog backup file to the relay path
	//cmd := fmt.Sprintf("tar xzf %s -C %s", d.Param.BinlogBackupFilePath, relayPath)
	cmd := fmt.Sprintf("cp -r %s %s", d.Param.BinlogBackupFilePath, relayPath)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
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
	sh := shellRunner.NewShellRunner(cmdname, args)
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
	sh := shellRunner.NewShellRunner(CmdName, args)
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
