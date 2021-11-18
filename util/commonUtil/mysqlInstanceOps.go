package commonUtil

import (
	"fmt"
	"path/filepath"
	"strings"
	"zetta_util/util/configParse"
)

type MysqlInstanceOps struct {
	MysqlEtcFile configParse.MysqlOptionFile
	DbaToolPath  string
	BinPath      string
	DirFetched   bool
}

func (m *MysqlInstanceOps) Init() {
	m.MysqlEtcFile.Parse()
}

func (m *MysqlInstanceOps) port() (string, error) {
	m.Init()
	p := m.MysqlEtcFile.Parameters["port"]
	if len(p) == 0 || p == "SINGLE_KEY" {
		return "", fmt.Errorf("can't find the port para in the etc file")
	}
	return p, nil
}

func (m *MysqlInstanceOps) IsAlive() error {
	m.Init()
	port, err := m.port()
	if err != nil {
		return err
	}
	cmd := fmt.Sprintf("ps -ef | grep %s | grep -v grep ", port)
	sh := NewShellRunner(cmd, make([]string, 0))
	err = sh.Run()
	if err != nil {
		return err
	}
	if len(sh.Stdout()) == 0 {
		return fmt.Errorf("mysqld %s may not alive", port)
	}

	if m.DirFetched == false {
		_ = m.FetchWorkingDir()
	}
	return nil

}

func (m *MysqlInstanceOps) StartMysqld() error {
	m.Init()
	port, err := m.port()
	if err != nil {
		return err
	}
	cmd := fmt.Sprintf("cd %s;./startmysql.sh %s", m.DbaToolPath, port)
	sh := NewShellRunner(cmd, make([]string, 0))
	err = sh.Run()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	return nil
}

func (m *MysqlInstanceOps) ShutDownByKill() (bool, error) {
	m.Init()
	port, err := m.port()
	if err != nil {
		return false, err
	}
	cmd := fmt.Sprintf("ps -ef | grep %s | grep -v grep | grep -v mysqld_safe |grep mysqld | grep socket| awk -F' ' '{printf $2 \" \" $3}' | xargs kill -9 ", port)
	sh := NewShellRunner(cmd, make([]string, 0))
	err = sh.Run()
	if err != nil {
		return false, fmt.Errorf("%s", sh.OutPut())
	}
	return true, nil
}
func (m *MysqlInstanceOps) FetchWorkingDir() error {
	m.Init()
	port, err := m.port()
	if err != nil {
		return err
	}
	var cmd = fmt.Sprintf("ps -ef | grep %s | grep -v grep | grep -v mysqld_safe| awk -F'--defaults-file' '{printf $1}'| awk -F' ' '{print $NF}'", port)
	sh := NewShellRunner(cmd, make([]string, 0))
	err = sh.Run()
	output := sh.Stdout()
	if err != nil {
		return fmt.Errorf("errorinfo: %s, %s", err.Error(), output)
	}

	binstr, _ := filepath.Split(output)

	m.BinPath = func(str string, suffix string) string {
		if strings.HasSuffix(str, suffix) {
			str = str[:len(str)-len(suffix)]
		}
		return str
	}(binstr, "/")

	installPath, _ := filepath.Split(m.BinPath)
	m.DbaToolPath = installPath + "dba_tools"
	m.DirFetched = true
	return nil
}
