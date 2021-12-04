/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package configParse

import (
	"fmt"
	"github.com/jmoiron/sqlx"
)

func NewMysqlConnectionBySockFile(sockFile string, user string, passwd string) (*sqlx.DB, error) {
	var err error
	url := fmt.Sprintf("%s:%s@unix(%s)/mysql", user, passwd, sockFile)
	conn, err := sqlx.Connect("mysql", url)
	//conn, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
func NewMysqlConnectionByTcp(ip string, port string, user string, passwd string) (*sqlx.DB, error) {
	var err error
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)", user, passwd, ip, port)
	conn, err := sqlx.Connect("mysql", url)
	//conn, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
