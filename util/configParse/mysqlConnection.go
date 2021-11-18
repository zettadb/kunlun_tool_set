package configParse

import (
	"database/sql"
	"fmt"
)

func NewMysqlConnectionBySockFile(sockFile string, user string, passwd string) (*sql.DB, error) {
	var err error
	url := fmt.Sprintf("%s:%s@unix(%s)/mysql", user, passwd, sockFile)
	conn, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
func NewMysqlConnectionByTcp(ip string, port string, user string, passwd string) (*sql.DB, error) {
	var err error
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)", user, passwd, ip, port)
	conn, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
