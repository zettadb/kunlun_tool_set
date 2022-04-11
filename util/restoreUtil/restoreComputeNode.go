package restoreUtil

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/metakeule/fmtdate"
	"path/filepath"
	"sort"
	"strings"
	"zetta_util/util/configParse"
	"zetta_util/util/logger"
	"zetta_util/util/remoteFileOps"
	"zetta_util/util/shellRunner"
)

type RestoreComputeNodeType struct {
	RtFile       string
	BinaryDir    string
	DataDir      string
	PgPort       string
	ClusterName  string
	ShardName    string
	PgConn       *sql.DB
	metaConn     *sql.DB
	metaOrigConn *sql.DB
}

func (r *RestoreComputeNodeType) parseBinaryDir(args *configParse.RestoreUtilArguments) error {
	cmd := fmt.Sprintf("ps -ef | grep %s | grep postgres | grep -v psql", args.Port)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}
	output := sh.Stdout()
	tokenVec := strings.Split(output, " ")
	s := len(tokenVec)
	r.DataDir = tokenVec[s-1]

	binaryAbsPath := tokenVec[s-3]
	r.BinaryDir = filepath.Dir(binaryAbsPath)
	r.PgPort = args.Port

	return nil
}

func (r *RestoreComputeNodeType) NewRemoteFileOperator(rtype string) *remoteFileOps.HdfsOperateType {
	// only hdfs
	return remoteFileOps.NewHdfsOperateType()
}

func (r *RestoreComputeNodeType) confirmFileByRtime(rtime string, filenamesVec []string) error {

	timePathMap := make(map[int64]string, 0)
	for _, lines := range filenamesVec {
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
	restoreTime, err := fmtdate.Parse("YYYY-MM-DD hh:mm:ss", rtime)
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
		return fmt.Errorf("latest cold back pgdump file is %s, some cold back may lost", timePathMap[key])
	}

	r.RtFile = timePathMap[key]
	return nil
}

func (r *RestoreComputeNodeType) restoreSeperateDir(dnVec []string) error {
	basedir := fmt.Sprintf("%s/pg_base", configParse.RestoreBaseDir)

	for _, dn := range dnVec {
		cmd := fmt.Sprintf("%s/pg_restore -h %s -p %s -d postgres %s",
			r.BinaryDir,
			r.DataDir,
			r.PgPort,
			fmt.Sprintf("%s/%s", basedir, dn),
		)
		sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
		err := sh.Run()
		if err != nil {
			return err
		}
	}

	return nil
}
func (r *RestoreComputeNodeType) initPgConn(args *configParse.RestoreUtilArguments) error {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=abc password=abc dbname=postgres sslmode=disable", r.DataDir, r.PgPort)
	var pgConn, err = sql.Open("postgres", psqlInfo)
	err = pgConn.Ping()
	if err != nil {
		logger.Log.Error(fmt.Sprintf("connect compute node error: %s", err.Error()))
		return err
	}
	r.PgConn = pgConn
	return nil
}
func (r *RestoreComputeNodeType) initMetaDataClusterConnection(args *configParse.RestoreUtilArguments) error {
	// init current metacluster conn
	err := r.initPgConn(args)
	if err != nil {
		return err
	}
	var pgConn = r.PgConn
	var ip, port, user, pass string

	sqlStr := fmt.Sprintf("select hostaddr,port,user_name,passwd from pg_catalog.pg_cluster_meta_nodes where is_master = true")
	rowResult, err := pgConn.Query(sqlStr)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("query metainfo from compute node error: %s", err.Error()))
		return err
	}
	rowResult.Next()
	err = rowResult.Scan(&ip, &port, &user, &pass)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("row scan to variables (ip port user pass) error:%s", err.Error()))
		return err
	}

	mysqlInfo := fmt.Sprintf("%s:%s@tcp(%s:%s)/mysql", user, pass, ip, port)
	metaConn, err := sql.Open("mysql", mysqlInfo)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("connect metadatacluster %s error: %s", mysqlInfo, err.Error()))
		return err
	}
	err = metaConn.Ping()
	if err != nil {
		logger.Log.Error(fmt.Sprintf("ping %s error: %s", mysqlInfo, err.Error()))
		return err
	}
	r.metaConn = metaConn

	// init orig metacluster connection
	origMetaConn, err := sql.Open("mysql", args.OrigMetaClusterConnStr)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("connect orig metacluster %s error: %s", args.OrigMetaClusterConnStr, err.Error()))
		return err
	}
	err = origMetaConn.Ping()
	if err != nil {
		logger.Log.Error(fmt.Sprintf("ping orig metacluster %s error: %s", args.OrigMetaClusterConnStr, err.Error()))
		return err
	}
	r.metaOrigConn = origMetaConn
	return nil

}
func (r *RestoreComputeNodeType) getCurrentClusterName(args *configParse.RestoreUtilArguments) error {
	sqlStr := fmt.Sprintf("select cluster_name from pg_catalog.pg_cluster_meta")
	rows, err := r.PgConn.Query(sqlStr)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("query %s error: %s", sqlStr, err.Error()))
		return err
	}
	rows.Next()
	err = rows.Scan(&r.ClusterName)
	if err != nil {
		logger.Log.Error("row scan (clustername) error: %s", err.Error())
		return err
	}
	return nil

}

func (r *RestoreComputeNodeType) copyDDLLogTableFromOrigToCurrent(args *configParse.RestoreUtilArguments) error {

	return nil
}

type ddlLogType struct {
	id             sql.NullInt16
	objName        sql.NullString
	dbName         sql.NullString
	schemaName     sql.NullString
	userName       sql.NullString
	roleName       sql.NullString
	searchPath     sql.NullString
	opType         sql.NullString
	objType        sql.NullString
	whenLogged     sql.NullString
	sqlSrc         sql.NullString
	sqlStorageNode sql.NullString
	targetShardId  sql.NullInt16
	initiator      sql.NullInt16
}

func (r *RestoreComputeNodeType) copyBackDDLlog(args *configParse.RestoreUtilArguments) error {
	origMetaDDLLogTableName := fmt.Sprintf("ddl_ops_log_%s", args.OrigClusterName)
	metaDDLLogTableName := fmt.Sprintf("ddl_ops_log_%s", r.ClusterName)

	sql1 := fmt.Sprintf("select * from kunlun_metadata_db.%s", origMetaDDLLogTableName)
	rows, err := r.metaOrigConn.Query(sql1)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("fetch ddl log from origMetadatacluster error: %s", err.Error()))
		return err
	}
	ddlEntryVec := make([]ddlLogType, 0)
	for rows.Next() {
		ddlRow := ddlLogType{}
		err := rows.Scan(
			&ddlRow.id,
			&ddlRow.objName,
			&ddlRow.dbName,
			&ddlRow.schemaName,
			&ddlRow.userName,
			&ddlRow.roleName,
			&ddlRow.searchPath,
			&ddlRow.opType,
			&ddlRow.objType,
			&ddlRow.whenLogged,
			&ddlRow.sqlSrc,
			&ddlRow.sqlStorageNode,
			&ddlRow.targetShardId,
			&ddlRow.initiator)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("row scan error %s", err.Error()))
			return err
		}
		ddlEntryVec = append(ddlEntryVec, ddlRow)
	}

	//after fetch, we insert these row to the new
	var result sql.Result
	for _, rowEntry := range ddlEntryVec {
		idValue, _ := rowEntry.id.Value()
		objname, _ := rowEntry.objName.Value()
		dbname, _ := rowEntry.dbName.Value()
		schemaname, _ := rowEntry.schemaName.Value()
		username, _ := rowEntry.userName.Value()
		rolename, _ := rowEntry.roleName.Value()
		searchPath, _ := rowEntry.searchPath.Value()
		optype, _ := rowEntry.opType.Value()
		objtype, _ := rowEntry.objType.Value()
		whenlog, _ := rowEntry.whenLogged.Value()
		sqlsrc, _ := rowEntry.sqlSrc.Value()
		sqlsrc_str := fmt.Sprintf("%s", sqlsrc)
		sqlstorage, _ := rowEntry.sqlStorageNode.Value()
		sqlstorage_str := fmt.Sprintf("%s", sqlstorage)
		targetshardid, _ := rowEntry.targetShardId.Value()
		//initiator, _ := rowEntry.initiator.Value()
		sql2 := fmt.Sprintf(
			`replace into kunlun_metadata_db.%s 
							set id = '%d', 
							objname = '%s',
							db_name = '%s',
							schema_name = '%s',
							user_name = '%s',
							role_name = '%s',
							search_path = '%s',
							optype = '%s',
							objtype = '%s',
							when_logged = '%s',
							sql_src = "%s",
							sql_storage_node = "%s",
							target_shard_id = '%d',
							initiator = '1024'`,
			metaDDLLogTableName,
			idValue,
			objname,
			dbname,
			schemaname,
			username,
			rolename,
			searchPath,
			optype,
			objtype,
			whenlog,
			strings.ReplaceAll(sqlsrc_str, "\"", "\\\""),
			strings.ReplaceAll(sqlstorage_str, "\"", "\\\""),
			targetshardid)
		//initiator)
		result, err = r.metaConn.Exec(sql2)
		if err != nil {
			logger.Log.Error(err.Error())
			logger.Log.Error(fmt.Sprintf("%s", sql2))
			return err
		}
	}
	// alter the autoincrement
	lastid, _ := result.LastInsertId()
	sql3 := fmt.Sprintf("alter table kunlun_metadata_db.%s auto_increment=%d", metaDDLLogTableName, lastid+10)
	_, err = r.metaConn.Exec(sql3)
	if err != nil {
		return err
	}
	return nil
}

func (r *RestoreComputeNodeType) prepareMetaDataMySQL(args *configParse.RestoreUtilArguments) error {

	// 1. fetch metadata cluster info of current compute node
	err := r.initMetaDataClusterConnection(args)
	if err != nil {
		return err
	}

	// 2. fetch current cluster name
	err = r.getCurrentClusterName(args)
	if err != nil {
		return err
	}
	//3. copy ddl_log table from orig metacluster to current meatacluster
	err = r.copyBackDDLlog(args)
	if err != nil {
		return err
	}

	return nil
}
func (r *RestoreComputeNodeType) restoreDumpedFile(args *configParse.RestoreUtilArguments) error {
	err := r.prepareMetaDataMySQL(args)
	if err != nil {
		return err
	}
	/*
		d := fmt.Sprintf("%s/pg_base", configParse.RestoreBaseDir)
		var dnVec []string

		dirname, err := ioutil.ReadDir(d)
		if err != nil {
			return err
		}
		for _, dn := range dirname {
			dnVec = append(
				dnVec,
				dn.Name(),
			)
		}
		err = r.restoreSeperateDir(dnVec)
		if err != nil {
			return err
		}
	*/
	return nil
}
func (r *RestoreComputeNodeType) FetchDatabaseListFromComputeNode() ([]string, error) {
	sql := fmt.Sprintf("select datname from pg_catalog.pg_database where datistemplate=false")
	rows, err := r.PgConn.Query(sql)
	if err != nil {
		return nil, err
	}
	dbnameVec := make([]string, 0)
	for rows.Next() {
		var db string
		err := rows.Scan(&db)
		if err != nil {
			return nil, err
		}
		dbnameVec = append(dbnameVec, db)
	}
	return dbnameVec, nil
}
func (r *RestoreComputeNodeType) InsertShardRemapInstructor(shardMap string) error {
	var lastTxnIdFromDDLlog string
	fetchLastTxnIdSql :=
		fmt.Sprintf("select txn from ddl_ops_log_%s order by id desc limit 1", r.ClusterName)
	rows, err := r.metaConn.Query(fetchLastTxnIdSql)
	if err != nil {
		return err
	}
	err = rows.Scan(&lastTxnIdFromDDLlog)
	if err != nil {
		return err
	}
	dbvec, err := r.FetchDatabaseListFromComputeNode()
	if err != nil {
		return err
	}
	for _, dbname := range dbvec {
		sqlbuf := fmt.Sprintf(
			"INSERT INTO kunlun_metadata_db.ddl_ops_log_%s "+
				"(objname, "+
				"db_name, "+
				"schema_name, "+
				"user_name, "+
				"role_name, "+
				"search_path, "+
				"optype, "+
				"objtype, "+
				"sql_src, "+
				"sql_storage_node, "+
				"target_shard_id, "+
				"initiator, "+
				"txn_id) "+
				"VALUES      "+
				"('', "+
				"'%s', "+
				"'postgres', "+
				"'postgres', "+
				"'none', "+
				"'\"$user\", public', "+
				"'remap_shardid', "+
				"'others', "+
				"'%s', "+
				"'', "+
				"0, "+
				"0, "+
				"'%s');",
			dbname, r.ClusterName, configParse.ShardMap, lastTxnIdFromDDLlog)

		_, err = r.metaConn.Exec(sqlbuf)
		if err != nil {
			return err
		}
	}

	return nil
}
func (r *RestoreComputeNodeType) RestoreComputeNode(args *configParse.RestoreUtilArguments) error {
	var err error
	err = r.parseBinaryDir(args)
	if err != nil {
		return err
	}
	/*
		var lsinfoVec []string
		//fill lsinfoVec
		cmd := fmt.Sprintf("hadoop fs -ls %s/pgdump/%s/%s/", configParse.HdfsBaseDir, args.OrigClusterName, args.OrigShardName)
		sh2 := shellRunner.NewShellRunner(cmd, make([]string, 0))
		err = sh2.Run()
		if err != nil {
			return err
		}
		rawOutPut := sh2.Stdout()
		lsinfoVec = strings.Split(rawOutPut, "\n")

		err = r.confirmFileByRtime(args.RestoreTime, lsinfoVec)
		if err != nil {
			logger.Log.Error(err.Error())
			return err
		}
		rt := r.NewRemoteFileOperator(args.RestoreType)
		hdfsrt := rt
		hdfsrt.Dir = filepath.Dir(r.RtFile)
		hdfsrt.Filename = filepath.Base(r.RtFile)

		dstTarball := fmt.Sprintf("%s/pg_dump.tgz", configParse.RestoreBaseDir)
		//fetch postgres cold back file from hdfs
		err = remoteFileOps.PullFromRemote(hdfsrt, dstTarball)
		if err != nil {
			return err
		}
		// untar the tarball
		cmd = fmt.Sprintf("cd %s;tar xzf pg_dump.tgz", configParse.RestoreBaseDir)
		sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
		err = sh.Run()
		if err != nil {
			return err
		}
	*/
	// do pg_restore
	err = r.restoreDumpedFile(args)
	if err != nil {
		return err
	}
	// Insert Shard ID remap instruction
	err = r.InsertShardRemapInstructor(configParse.ShardMap)
	if err != nil {
		return err
	}

	return nil

}
