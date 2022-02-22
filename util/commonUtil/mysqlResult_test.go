package commonUtil

import (
	"database/sql"
	"fmt"
	"testing"
)
import _ "github.com/go-sql-driver/mysql"

func TestMySQLResult(t *testing.T) {
	var err error
	connStr := fmt.Sprintf("sbtest:sbtest@(192.168.0.135:9002)/mysql")
	MysqlConnection, err := sql.Open("mysql", connStr)
	if err != nil {
		t.Errorf(fmt.Sprintf("Open source MySQL connection failed: %s", err.Error()))
		t.Fail()
	}
	sql := fmt.Sprintf("select * from mysql.user")
	rows, err := MysqlConnection.Query(sql)
	mysqlResult := MySQLQueryResult{columnName: nil, rowValue: nil}
	err = mysqlResult.Transfer(rows)
	if err != nil {
		t.Errorf(err.Error())
	}
	rowsNum := mysqlResult.RowsNum()
	for index := 0; index < rowsNum; index++ {
		t.Log(mysqlResult.GetValueString(index, "User").String)
	}
}
