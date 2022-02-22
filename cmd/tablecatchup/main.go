package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strings"
	"time"
	"zetta_util/util/commonUtil"
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

//type slaveStatusInfo struct {
//	Slave_IO_State                sql.NullString
//	Master_Host                   sql.NullString
//	Master_User                   sql.NullString
//	Master_Port                   sql.NullString
//	Connect_Retry                 sql.NullString
//	Master_Log_File               sql.NullString
//	Read_Master_Log_Pos           sql.NullString
//	Relay_Log_File                sql.NullString
//	Relay_Log_Pos                 sql.NullString
//	Relay_Master_Log_File         sql.NullString
//	Slave_IO_Running              sql.NullString
//	Slave_SQL_Running             sql.NullString
//	Replicate_Do_DB               sql.NullString
//	Replicate_Ignore_DB           sql.NullString
//	Replicate_Do_Table            sql.NullString
//	Replicate_Ignore_Table        sql.NullString
//	Replicate_Wild_Do_Table       sql.NullString
//	Replicate_Wild_Ignore_Table   sql.NullString
//	Last_Errno                    sql.NullString
//	Last_Error                    sql.NullString
//	Skip_Counter                  sql.NullString
//	Exec_Master_Log_Pos           sql.NullString
//	Relay_Log_Space               sql.NullString
//	Until_Condition               sql.NullString
//	Until_Log_File                sql.NullString
//	Until_Log_Pos                 sql.NullString
//	Master_SSL_Allowed            sql.NullString
//	Master_SSL_CA_File            sql.NullString
//	Master_SSL_CA_Path            sql.NullString
//	Master_SSL_Cert               sql.NullString
//	Master_SSL_Cipher             sql.NullString
//	Master_SSL_Key                sql.NullString
//	Seconds_Behind_Master         sql.NullString
//	Master_SSL_Verify_Server_Cert sql.NullString
//	Last_IO_Errno                 sql.NullString
//	Last_IO_Error                 sql.NullString
//	Last_SQL_Errno                sql.NullString
//	Last_SQL_Error                sql.NullString
//	Replicate_Ignore_Server_Ids   sql.NullString
//	Master_Server_Id              sql.NullString
//	Master_UUID                   sql.NullString
//	Master_Info_File              sql.NullString
//	SQL_Delay                     sql.NullString
//	SQL_Remaining_Delay           sql.NullString
//	Slave_SQL_Running_State       sql.NullString
//	Master_Retry_Count            sql.NullString
//	Master_Bind                   sql.NullString
//	Last_IO_Error_Timestamp       sql.NullString
//	Last_SQL_Error_Timestamp      sql.NullString
//	Master_SSL_Crl                sql.NullString
//	Master_SSL_Crlpath            sql.NullString
//	Retrieved_Gtid_Set            sql.NullString
//	Executed_Gtid_Set             sql.NullString
//	Auto_Position                 sql.NullString
//	Replicate_Rewrite_DB          sql.NullString
//	Channel_Name                  sql.NullString
//	Master_TLS_Version            sql.NullString
//	Master_public_key_path        sql.NullString
//	Get_master_public_key         sql.NullString
//	Network_Namespace             sql.NullString
//}
type paraMysql struct {
	addr      string
	port      int
	user      string
	pass      string
	tableList string
}
type tableSpec struct {
	dbName    string
	tableName string
}

var tableSpecVec []tableSpec

func finishSync(rows sql.Rows) bool {
	result := commonUtil.MySQLQueryResult{}
	err := result.Transfer(&rows)
	if err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(-1)
	}
	SlaveSqlRunningState := result.GetValueString(0, "Slave_SQL_Running_State")
	SlaveSqlRunning := result.GetValueString(0, "Slave_SQL_Running")
	if SlaveSqlRunningState.String == "Slave has read all relay log; waiting for more updates" && SlaveSqlRunning.String == "Yes" {
		return true
	}
	return false
}

//func finishSync(rows sql.Rows) bool {
//
//	slaveInfo := slaveStatusInfo{}
//	for rows.Next() {
//		err := rows.Scan(
//			&slaveInfo.Slave_IO_State,
//			&slaveInfo.Master_Host,
//			&slaveInfo.Master_User,
//			&slaveInfo.Master_Port,
//			&slaveInfo.Connect_Retry,
//			&slaveInfo.Master_Log_File,
//			&slaveInfo.Read_Master_Log_Pos,
//			&slaveInfo.Relay_Log_File,
//			&slaveInfo.Relay_Log_Pos,
//			&slaveInfo.Relay_Master_Log_File,
//			&slaveInfo.Slave_IO_Running,
//			&slaveInfo.Slave_SQL_Running,
//			&slaveInfo.Replicate_Do_DB,
//			&slaveInfo.Replicate_Ignore_DB,
//			&slaveInfo.Replicate_Do_Table,
//			&slaveInfo.Replicate_Ignore_Table,
//			&slaveInfo.Replicate_Wild_Do_Table,
//			&slaveInfo.Replicate_Wild_Ignore_Table,
//			&slaveInfo.Last_Errno,
//			&slaveInfo.Last_Error,
//			&slaveInfo.Skip_Counter,
//			&slaveInfo.Exec_Master_Log_Pos,
//			&slaveInfo.Relay_Log_Space,
//			&slaveInfo.Until_Condition,
//			&slaveInfo.Until_Log_File,
//			&slaveInfo.Until_Log_Pos,
//			&slaveInfo.Master_SSL_Allowed,
//			&slaveInfo.Master_SSL_CA_File,
//			&slaveInfo.Master_SSL_CA_Path,
//			&slaveInfo.Master_SSL_Cert,
//			&slaveInfo.Master_SSL_Cipher,
//			&slaveInfo.Master_SSL_Key,
//			&slaveInfo.Seconds_Behind_Master,
//			&slaveInfo.Master_SSL_Verify_Server_Cert,
//			&slaveInfo.Last_IO_Errno,
//			&slaveInfo.Last_IO_Error,
//			&slaveInfo.Last_SQL_Errno,
//			&slaveInfo.Last_SQL_Error,
//			&slaveInfo.Replicate_Ignore_Server_Ids,
//			&slaveInfo.Master_Server_Id,
//			&slaveInfo.Master_UUID,
//			&slaveInfo.Master_Info_File,
//			&slaveInfo.SQL_Delay,
//			&slaveInfo.SQL_Remaining_Delay,
//			&slaveInfo.Slave_SQL_Running_State,
//			&slaveInfo.Master_Retry_Count,
//			&slaveInfo.Master_Bind,
//			&slaveInfo.Last_IO_Error_Timestamp,
//			&slaveInfo.Last_SQL_Error_Timestamp,
//			&slaveInfo.Master_SSL_Crl,
//			&slaveInfo.Master_SSL_Crlpath,
//			&slaveInfo.Retrieved_Gtid_Set,
//			&slaveInfo.Executed_Gtid_Set,
//			&slaveInfo.Auto_Position,
//			&slaveInfo.Replicate_Rewrite_DB,
//			&slaveInfo.Channel_Name,
//			&slaveInfo.Master_TLS_Version,
//			&slaveInfo.Master_public_key_path,
//			&slaveInfo.Get_master_public_key,
//			&slaveInfo.Network_Namespace,
//		)
//		if err != nil {
//			os.Stderr.WriteString(err.Error())
//			os.Exit(-1)
//		}
//	}
//	if slaveInfo.Slave_SQL_Running_State.String == "Slave has read all relay log; waiting for more updates" &&
//		slaveInfo.Slave_SQL_Running.String == "Yes" {
//		return true
//	}
//	return false
//}

func renameTable(mysqlConnection *sql.DB, dbName string, tableName string) error {
	var renameStmt = fmt.Sprintf("rename table %s.%s to %s.%s_expand",
		dbName, tableName, dbName, tableName)
	for {
		//todo: add retry number logic here
		_, err := mysqlConnection.Exec(renameStmt)
		if err != nil {
			//todo: add kill session logic here
			//		time.Sleep(1 * time.Second)
			return err
		} else {
			break
		}
	}
	return nil
}

func getSlaveStat(ctx context.Context, dstMysql *sql.DB, pipe chan sql.Rows) {
	for {

		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(1 * time.Second)
		}

		var showSlaveStmt = fmt.Sprintf("show slave status")
		result, err := dstMysql.Query(showSlaveStmt)
		if err != nil {
			return
		}
		pipe <- *result
	}
}

func doClean(mysqlConn *sql.DB) error {
	stopSlaveStmt := fmt.Sprintf("stop slave for channel 'expand';")
	_, err := mysqlConn.Exec(stopSlaveStmt)
	if err != nil {
		return err
	}
	resetSlaveAllStmt := fmt.Sprintf("reset slave all;")
	_, err = mysqlConn.Exec(resetSlaveAllStmt)
	if err != nil {
		return err
	}
	return nil
}

func buildChannelToSync(para *paraMysql, srcMysql *sql.DB, dstMysql *sql.DB, tableList string, gtidInfo string) (bool, error) {
	//create channel on target MySQL instance point to source MySQL

	//Step1: provisioning the gtid related info
	var resetMasterStmt = fmt.Sprintf("reset master")
	var setGtidPurged = fmt.Sprintf("SET @@GLOBAL.gtid_purged = \"%s\"", gtidInfo)

	_, err := dstMysql.Exec(resetMasterStmt)
	if err != nil {
		return false, err
	}
	_, err = dstMysql.Exec(setGtidPurged)
	if err != nil {
		return false, err
	}

	//Step2: create channel to the source MySQL instance
	var changeMasterStmt = fmt.Sprintf("change master to master_host=\"%s\",master_port=%d,"+
		"master_user=\"%s\",master_password=\"%s\","+
		"master_auto_position=1 for channel 'expand'",
		para.addr, para.port, para.user, para.pass)

	_, err = dstMysql.Exec(changeMasterStmt)
	if err != nil {
		return false, err
	}

	var changeChannelFilter = fmt.Sprintf("change replication filter "+
		"REPLICATE_DO_TABLE=(%s) for channel 'expand'",
		para.tableList)

	_, err = dstMysql.Exec(changeChannelFilter)
	if err != nil {
		return false, err
	}

	//Step3: start the replicate slave
	var startSlaveStmt = fmt.Sprintf("start slave for channel 'expand'")

	_, err = dstMysql.Exec(startSlaveStmt)
	if err != nil {
		return false, err
	}

	// loop check finish
	pipe := make(chan sql.Rows, 5)
	ctx, cancel := context.WithCancel(context.Background())
	go getSlaveStat(ctx, dstMysql, pipe)
	for {
		var finished bool
		finished = false
		select {
		case rows := <-pipe:
			{
				if finishSync(rows) {
					cancel()
					finished = true
					break
				}
			}
		default:
			time.Sleep(1 * time.Second)
		}
		if finished {
			break
		}
	}

	// do rename operation on source MySQL
	for _, item := range tableSpecVec {
		err = renameTable(srcMysql, item.dbName, item.tableName)
		if err != nil {
			return false, err
		}
	}

	// clean
	doClean(dstMysql)

	return true, nil
}
func parseTableInfo(tableInfo string, tableSpecVec *[]tableSpec) error {
	dbTables := strings.Split(tableInfo, ",")
	for _, item := range dbTables {
		info := strings.Split(item, ".")
		if len(info) != 2 {
			return fmt.Errorf("table info format is not valid, should be db.table[,db.table]\n")
		}
		spec := tableSpec{dbName: info[0], tableName: info[1]}
		*tableSpecVec = append(*tableSpecVec, spec)
	}
	return nil
}
func printIntro() {
	fmt.Println(Banner)
	intro := `
NAME
	tablecatchup - table sync between different MySQL instance

DESCRIPTION
	This is the tool be used in cluster expand to sync tables between MySQL instances.

OVERVIEW
`
	fmt.Println(intro)

}

func fetchGtidInfo(metaFilePath string) (string, error) {

	cmd := fmt.Sprintf("cat %s/metadata | grep GTID |awk -F 'GTID:' '{print $2}'", metaFilePath)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return "", err
	}
	return sh.Sh.Status.Stdout, nil

}
func main() {
	// flag define and parse
	var srcAddr = flag.String("src_addr", "", "source MySQL instance host address")
	var srcPort = flag.Int("src_port", 3306, "source MySQL instance host port")
	var srcUser = flag.String("src_user", "", "user account to connect source MySQL instance, REPL privilege is required")
	var srcPass = flag.String("src_pass", "", "password to connect source MySQL instance")

	var dstAddr = flag.String("dst_addr", "", "target MySQL instance host address")
	var dstPort = flag.Int("dst_port", 3306, "target MySQL instance host port")
	var dstUser = flag.String("dst_user", "", "target account to connect source MySQL instance, REPL privilege is required")
	var dstPass = flag.String("dst_pass", "", "target to connect source MySQL instance")

	var tableList = flag.String("table_list", "", "tables info")

	var gtidInfo = flag.String("gtid_info", "", "Executed_Gtid set info")
	var mydumperMetadataFile = flag.String("mydumper_metadata_file", "", "mydumper metadata file path")
	flag.Parse()
	if len(os.Args) < 2 {
		printIntro()
		flag.PrintDefaults()
		os.Exit(-1)
	}
	err := parseTableInfo(*tableList, &tableSpecVec)
	if err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(-1)
	}
	*gtidInfo, err = fetchGtidInfo(*mydumperMetadataFile)
	if err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(-1)
	}

	// init MySQL connection
	connStr := fmt.Sprintf("%s:%s@(%s:%d)/mysql", *srcUser, *srcPass, *srcAddr, *srcPort)
	srcMysqlConnection, err := sql.Open("mysql", connStr)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Open source MySQL connection failed: %s", err.Error()))
		os.Exit(-1)
	}

	connStr = fmt.Sprintf("%s:%s@(%s:%d)/mysql", *dstUser, *dstPass, *dstAddr, *dstPort)
	dstMysqlConnection, err := sql.Open("mysql", connStr)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Open target MySQL connection failed: %s", err.Error()))
		os.Exit(-1)
	}

	srcargs := paraMysql{addr: *srcAddr, port: *srcPort, user: *srcUser, pass: *srcPass, tableList: *tableList}
	// build replica
	_, err = buildChannelToSync(&srcargs, srcMysqlConnection, dstMysqlConnection, *tableList, *gtidInfo)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Sync by MySQL replication failed: %s\n", err.Error()))
		doClean(dstMysqlConnection)
		os.Exit(-1)
	}

	os.Exit(0)
}
