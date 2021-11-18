package restoreUtil

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
)

type commitLog struct {
	CompNodeId string
	TxnId      int64
	NextTxnCmd string
	PrepareTs  string
	ValueStr   string
}

func (l *commitLog) toString() {
	l.ValueStr = fmt.Sprintf("%s;%d;%s;%s\n",
		l.CompNodeId,
		l.TxnId,
		l.NextTxnCmd,
		l.PrepareTs)
}

type CommitLoggerProcessor struct {
	OrgClusterId    string
	ClogPath        string
	MetaConnString  string
	RestoreTime     string
	metaConnHandler *sql.DB
	Inited          bool
}

func (c *CommitLoggerProcessor) commitLogHandlerInit() error {
	if c.Inited == true {
		return nil
	}
	var err error
	c.Inited = true
	//_ = os.MkdirAll(c.ClogPath, 0755)
	c.metaConnHandler, err = sql.Open("mysql", c.MetaConnString)
	if err != nil {
		return err
	}
	return nil
}

func (c *CommitLoggerProcessor) PrepareCommitLogEntryToFile() error {
	var err error
	var query string
	err = c.commitLogHandlerInit()
	if err != nil {
		return err
	}
	if len(c.RestoreTime) == 0 {
		return fmt.Errorf("restoretime should not be specified if globalconsistent param is true")
	} else {
		query = fmt.Sprintf(
			"SELECT * FROM `kunlun_metadata_db`.`commit_log_%s` where prepare_ts <= '%s'",
			c.OrgClusterId,
			c.RestoreTime)
	}

	rows, err := c.metaConnHandler.Query(query)
	if err != nil {
		return err
	}

	clogFileHdl, err := os.Create(c.ClogPath)
	if err != nil {
		return err
	}

	var logItem commitLog
	for rows.Next() {
		err := rows.Scan(&logItem.CompNodeId, &logItem.TxnId, &logItem.NextTxnCmd, &logItem.PrepareTs)
		if err == nil {
			logItem.toString()
			_, err := clogFileHdl.WriteString(logItem.ValueStr)
			if err != nil {
				return err
			}
		}
	}
	err = clogFileHdl.Sync()
	if err != nil {
		return err
	}
	return nil
}
