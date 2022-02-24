package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"os"
	"strconv"
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

type paraMysql struct {
	addr      string
	port      int
	user      string
	pass      string
	tableList string
}
type paraPg struct {
	addr string
	port int
	user string
	pass string
}
type tableSpec struct {
	dbInfo     string
	dbName     string
	schemaName string
	tableName  string
}
type masterFilePos struct {
	file string
	pos  string
}

func (m *masterFilePos) Parse(metaFilePath string) error {
	cmd := fmt.Sprintf("cat %s | grep Log |awk -F 'Log: ' '{print $2}'", metaFilePath)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}
	m.file = strings.TrimSuffix(sh.Sh.Status.Stdout, "\n")
	cmd = fmt.Sprintf("cat %s | grep Pos |awk -F 'Pos: ' '{print $2}'", metaFilePath)
	sh1 := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err = sh1.Run()
	if err != nil {
		return err
	}
	m.pos = strings.TrimSuffix(sh1.Sh.Status.Stdout, "\n")
	return nil
}

var FilePos masterFilePos

var tableSpecVec []tableSpec

func finishSync(rows sql.Rows) bool {
	result := commonUtil.MySQLQueryResult{}
	err := result.Transfer(&rows)
	if nil != err {
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

func renameTableOnTarget(mysqlConnection *sql.DB, dbName string, tableName string, suffixInfo string) error {
	var renameStmt = fmt.Sprintf("rename table %s.%s_expand_%s to %s.%s",
		dbName, tableName, suffixInfo, dbName, tableName)
	for {
		//todo: add retry number logic here
		_, err := mysqlConnection.Exec(renameStmt)
		if err != nil {
			time.Sleep(2 * time.Second)
		} else {
			break
		}
	}
	return nil
}

func renameTable(srcMysql *sql.DB, dstMysql *sql.DB, dbName string, tableName string, suffixInfo string) error {
	var renameStmt = fmt.Sprintf("rename table %s.%s to %s.%s_expand_%s",
		dbName, tableName, dbName, tableName, suffixInfo)
	for {
		//todo: add retry number logic here
		_, err := srcMysql.Exec(renameStmt)
		if err != nil {
			//todo: add kill session logic here
			//		time.Sleep(1 * time.Second)
			return fmt.Errorf("sql:%s err:%s", renameStmt, err.Error())
		} else {
			break
		}
	}
	time.Sleep(1 * time.Second)
	// check target MySQL instance has already sync the statement
	var checkStmt = fmt.Sprintf("show create table %s.%s_expand_%s", dbName, tableName, suffixInfo)
	_, err := dstMysql.Exec(checkStmt)
	if err != nil {
		return fmt.Errorf("target MySQL didn't sync the rename stmt: %s", err.Error())
	}
	return nil
}

func getSlaveStat(ctx context.Context, dstMysql *sql.DB, pipe chan sql.Rows, expandInfoSuffix string) {
	for {

		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(1 * time.Second)
		}

		var showSlaveStmt = fmt.Sprintf("show slave status for channel 'expand_%s'", expandInfoSuffix)
		result, err := dstMysql.Query(showSlaveStmt)
		if err != nil {
			return
		}
		pipe <- *result
	}
}

func doClean(mysqlConn *sql.DB, expandInfoSuffix string) error {
	stopSlaveStmt := fmt.Sprintf("stop slave for channel 'expand_%s'", expandInfoSuffix)
	_, err := mysqlConn.Exec(stopSlaveStmt)
	if err != nil {
		return err
	}
	resetSlaveAllStmt := fmt.Sprintf("reset slave all for channel 'expand_%s'", expandInfoSuffix)
	_, err = mysqlConn.Exec(resetSlaveAllStmt)
	if err != nil {
		return err
	}
	return nil
}

func buildChannelToSync(para *paraMysql, dstMysql *sql.DB, expandInfoSuffix string) (bool, error) {
	//create channel on target MySQL instance point to source MySQL

	//Step1: create channel to the source MySQL instance
	var changeMasterStmt = fmt.Sprintf("change master to master_host='%s',master_port=%d,"+
		"master_user='%s',master_password='%s', master_log_file='%s', master_log_pos = %s ,"+
		"MASTER_HEARTBEAT_PERIOD=3 for channel 'expand_%s'",
		para.addr, para.port, para.user, para.pass, FilePos.file, FilePos.pos, expandInfoSuffix)

	_, err := dstMysql.Exec(changeMasterStmt)
	if err != nil {
		return false, err
	}

	var tableList string
	for index, item := range tableSpecVec {
		if index == 0 {
			tableList += item.dbName + "." + item.tableName
		} else {
			tableList += ","
			tableList += item.dbName + "." + item.tableName
		}
	}
	var changeChannelFilter = fmt.Sprintf("change replication filter "+
		"REPLICATE_DO_TABLE=(%s) for channel 'expand_%s'",
		tableList, expandInfoSuffix)

	_, err = dstMysql.Exec(changeChannelFilter)
	if err != nil {
		return false, err
	}

	//Step2: start the replicate slave
	var startSlaveStmt = fmt.Sprintf("start slave for channel 'expand_%s'", expandInfoSuffix)

	_, err = dstMysql.Exec(startSlaveStmt)
	if err != nil {
		return false, err
	}

	// loop check finish
	pipe := make(chan sql.Rows, 5)
	ctx, cancel := context.WithCancel(context.Background())
	go getSlaveStat(ctx, dstMysql, pipe, expandInfoSuffix)
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

	return true, nil
}
func parseTableInfo(tableInfo string, tableSpecVec *[]tableSpec) error {
	dbTables := strings.Split(tableInfo, ",")
	for _, item := range dbTables {
		info := strings.Split(item, ".")
		if len(info) != 3 {
			return fmt.Errorf("table info format is not valid, should be db.table[,db.schema.table]\n")
		}
		spec := tableSpec{dbInfo: info[0], schemaName: info[1], tableName: info[2]}
		spec.dbName = spec.dbInfo + "_$$_" + spec.schemaName

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

	cmd := fmt.Sprintf("cat %s | grep GTID |awk -F 'GTID:' '{print $2}'", metaFilePath)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(sh.Sh.Status.Stdout, "\n"), nil
}

func validateExpandSuffix(origInfo string) string {
	var realInfo string = ""
	if origInfo == "unix_timestamp" {
		realInfo = strconv.FormatInt(time.Now().Unix(), 10)
	} else {
		realInfo = origInfo
	}
	return realInfo
}

func reRoute(metaConnection *sql.DB, clusterId string, srcShardId string, dstShardId string) error {

	//fetch comp node info
	pgVec := make([]paraPg, 0)
	sqlStmt := fmt.Sprintf("select * from kunlun_metadata_db.comp_nodes where db_cluster_id = %s", clusterId)
	rows, err := metaConnection.Query(sqlStmt)
	if err != nil {
		return err
	}
	result := commonUtil.MySQLQueryResult{}
	err = result.Transfer(rows)
	if err != nil {
		return err
	}
	rowNum := result.RowsNum()
	if rowNum == 0 {
		return fmt.Errorf("can not find comp node info in kunlun_metadata_db.comp_nodes where db_cluster_id is %s", clusterId)
	}
	for i := 0; i < rowNum; i++ {
		portTmp, _ := strconv.Atoi(result.GetValueString(i, "port").String)
		pgPara := paraPg{
			addr: result.GetValueString(i, "hostaddr").String,
			port: portTmp,
			user: result.GetValueString(i, "user_name").String,
			pass: result.GetValueString(i, "passwd").String,
		}
		pgVec = append(pgVec, pgPara)
	}

	for _, itempg := range pgVec {
		//modify the relshardid
		for _, item := range tableSpecVec {
			//connect and do modify the relshardid
			pgUrl := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
				itempg.addr, itempg.port, itempg.user, itempg.pass, item.dbInfo)
			pgConn, err := sql.Open("postgres", pgUrl)
			if err != nil || pgConn.Ping() != nil {
				continue
			}
			pgSql := fmt.Sprintf(
				"update pg_catalog.pg_class set relshardid=%s where"+
					" relname='%s' and relshardid=%s and relnamespace"+
					"=(select oid from pg_namespace where nspname='%s')",
				dstShardId, item.tableName, srcShardId, item.schemaName)

			//todo: deal the error scenario
			pgConn.Exec(pgSql)
			pgConn.Close()
		}
	}

	return nil
}

func main() {
	// flag define and parse

	var srcShardId = flag.String("src_shard_id", "", "source MySQL cluster id")
	var srcAddr = flag.String("src_addr", "", "source MySQL instance host address")
	var srcPort = flag.Int("src_port", 3306, "source MySQL instance host port")
	var srcUser = flag.String("src_user", "", "user account to connect source MySQL instance, REPL privilege is required")
	var srcPass = flag.String("src_pass", "", "password to connect source MySQL instance")

	var dstShardId = flag.String("dst_shard_id", "", "target MySQL cluster id")
	var dstAddr = flag.String("dst_addr", "", "target MySQL instance host address")
	var dstPort = flag.Int("dst_port", 3306, "target MySQL instance host port")
	var dstUser = flag.String("dst_user", "", "target account to connect source MySQL instance, REPL privilege is required")
	var dstPass = flag.String("dst_pass", "", "target to connect source MySQL instance")

	var metaUrl = flag.String("meta_url", "pgx:pgx_pwd@(127.0.0.1:3306)/mysql", "connection url point to metadata cluster")
	var clusterId = flag.String("cluster_id", "", "kunlun cluster id")

	var tableList = flag.String("table_list", "", "tables info")
	var expandInfoSuffix = flag.String("expand_info_suffix", "unix_timestamp", "info specified to indicate the expand procedure")

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
	err = FilePos.Parse(*mydumperMetadataFile)
	if err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(-1)
	}
	*expandInfoSuffix = validateExpandSuffix(*expandInfoSuffix)

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

	metaMysqlConnection, err := sql.Open("mysql", *metaUrl)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Open metadata MySQL connection failed: %s", err.Error()))
		os.Exit(-1)
	}

	srcArgs := paraMysql{addr: *srcAddr, port: *srcPort, user: *srcUser, pass: *srcPass, tableList: *tableList}
	// build replica
	_, err = buildChannelToSync(&srcArgs, dstMysqlConnection, *expandInfoSuffix)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Sync by MySQL replication failed: %s\n", err.Error()))
		doClean(dstMysqlConnection, *expandInfoSuffix)
		os.Exit(-1)
	}

	// do rename operation on source MySQL
	for _, item := range tableSpecVec {
		err = renameTable(srcMysqlConnection, dstMysqlConnection, item.dbName, item.tableName, *expandInfoSuffix)
		if err != nil {
			os.Stderr.WriteString(err.Error())
			doClean(dstMysqlConnection, *expandInfoSuffix)
			os.Exit(-1)
		}
	}
	// reroute
	err = reRoute(metaMysqlConnection, *clusterId, *srcShardId, *dstShardId)
	if err != nil {
		os.Stderr.WriteString(err.Error())
		doClean(dstMysqlConnection, *expandInfoSuffix)
		os.Exit(-1)
	}

	//do rename operation on target MySQL
	for _, item := range tableSpecVec {
		renameTableOnTarget(dstMysqlConnection, item.dbName, item.tableName, *expandInfoSuffix)
	}

	// clean
	doClean(dstMysqlConnection, *expandInfoSuffix)

	os.Exit(0)
}
