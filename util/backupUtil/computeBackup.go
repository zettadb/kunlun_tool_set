/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package backupUtil

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"zetta_util/util/configParse"
	"zetta_util/util/logger"
	"zetta_util/util/remoteFileOps"
	"zetta_util/util/shellRunner"
)
import _ "github.com/lib/pq"

type DoPostgresBackupType struct {
	Databases    []string
	StoreBaseDir string
	BinaryDir    string
	DataDir      string
}

func (d *DoPostgresBackupType) FetchDatabaseName(args *configParse.BackupUtilArguments) error {
	db, err := sql.Open("postgres", fmt.Sprintf("user=abc password=abc host=192.168.0.135 port=%s dbname=postgres sslmode=disable", args.PgPort))
	if err != nil {
		return err
	}
	rows, err := db.Query("select datname from pg_catalog.pg_database")
	if err != nil {
		return err
	}
	for rows.Next() {
		var datname string
		err := rows.Scan(&datname)
		if err != nil {
			return err
		}
		if datname == "postgres" || datname == "template0" || datname == "template1" {
			continue
		}
		d.Databases = append(d.Databases, datname)
	}
	return nil
}

func (d *DoPostgresBackupType) parseBinaryDir(args *configParse.BackupUtilArguments) error {
	cmd := fmt.Sprintf("ps -ef | grep %s | grep postgres | grep -v psql", args.PgPort)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}
	output := sh.Stdout()
	tokenVec := strings.Split(output, " ")
	s := len(tokenVec)
	d.DataDir = tokenVec[s-1]

	binaryAbsPath := tokenVec[s-3]
	d.BinaryDir = filepath.Dir(binaryAbsPath)

	return nil

}

func (d *DoPostgresBackupType) dumpPGCataLog(args *configParse.BackupUtilArguments) error {
	pgDump := fmt.Sprintf("%s/pg_dump", d.BinaryDir)
	cmd := fmt.Sprintf("%s -h %s -p %s -b postgres -n pg_catalog -t pg_ddl_log_progress -a -Fd -f %s/pg_catalog ",
		pgDump, d.DataDir, args.PgPort, d.StoreBaseDir)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		logger.Log.Error(err.Error())
		return err
	}
	return nil
}

func (d *DoPostgresBackupType) dumpEachDatabase(dbname string, args *configParse.BackupUtilArguments) error {
	pgDump := fmt.Sprintf("%s/pg_dump", d.BinaryDir)
	cmd := fmt.Sprintf("%s -h %s -p %s -b %s -s -c -C -Fd -f %s/pg_db_%s",
		pgDump, d.DataDir, args.PgPort, dbname, d.StoreBaseDir, dbname)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}
	return nil
}

func (d *DoPostgresBackupType) dumpDatabases(args *configParse.BackupUtilArguments) error {
	for _, db := range d.Databases {
		err := d.dumpEachDatabase(db, args)
		if err != nil {
			logger.Log.Error(err.Error())
			return err
		}
	}
	return nil
}
func (d *DoPostgresBackupType) TearDown(args *configParse.BackupUtilArguments) error {
	// make tar ball
	cmd := fmt.Sprintf("cd %s;tar czf pg_base.tgz pg_base",
		configParse.BackupBaseDir)
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}

	hdfsHandler := remoteFileOps.NewHdfsOperateType()
	hdfsHandler.Dir = fmt.Sprintf("%s/pgdump/%s/%s",
		configParse.HdfsBaseDir,
		args.ClusterName,
		args.ShardName)
	hdfsHandler.Filename = fmt.Sprintf("_pgdump_%s_.tgz",
		time.Now().Format("D2006#01#02_T15#04#05"))
	remoteFileOps.PushToRemote(hdfsHandler, fmt.Sprintf("%s/pg_base.tgz", configParse.BackupBaseDir))

	return nil
}

func (d *DoPostgresBackupType) ColdBackup(arguments *configParse.BackupUtilArguments) error {
	// fetch databases from postgresql
	var err error
	d.StoreBaseDir = fmt.Sprintf("%s/pg_base", configParse.BackupBaseDir)
	_ = os.MkdirAll(d.StoreBaseDir, 0755)
	err = d.parseBinaryDir(arguments)
	if err != nil {
		return err
	}
	err = d.FetchDatabaseName(arguments)
	if err != nil {
		return err
	}
	err = d.dumpPGCataLog(arguments)
	if err != nil {
		return err
	}
	err = d.dumpDatabases(arguments)
	if err != nil {
		return err
	}
	err = d.TearDown(arguments)

	return nil
}

func (d *DoPostgresBackupType) IncreamentalLogBackup(arguments *configParse.BackupUtilArguments) error {
	return nil
}
