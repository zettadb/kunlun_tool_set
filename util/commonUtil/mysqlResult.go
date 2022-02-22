package commonUtil

import "database/sql"
import _ "github.com/go-sql-driver/mysql"

type MySQLQueryResult struct {
	columnName map[string]int
	rowValue   [][]sql.NullString
}

func (result *MySQLQueryResult) RowsNum() int {
	return len(result.rowValue)
}
func (result *MySQLQueryResult) GetValueString(index int, column string) sql.NullString {
	return result.rowValue[index][result.columnName[column]]
}

func (result *MySQLQueryResult) Transfer(rawRows *sql.Rows) error {
	// init column name table
	var err error = nil
	result.columnName = make(map[string]int)
	result.rowValue = make([][]sql.NullString, 0)
	columns, _ := rawRows.Columns()
	columnNum := len(columns)
	for index, name := range columns {
		result.columnName[name] = index
	}
	var catch = make([]interface{}, columnNum)
	for index, _ := range catch {
		//Scan() need the pointer type
		var a sql.NullString
		catch[index] = &a
	}
	for rawRows.Next() {
		err = rawRows.Scan(catch...)
		if err != nil {
			return err
		}
		rowItem := make([]sql.NullString, columnNum)
		for i, d := range catch {
			rowItem[i] = *(d.(*sql.NullString))
		}
		result.rowValue = append(result.rowValue, rowItem)
	}
	return nil
}
