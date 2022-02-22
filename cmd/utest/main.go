package main

import (
	"fmt"
	"os"
	"time"
	"zetta_util/util/shellRunner"
)

func main() {

	cmd := "xtrabackup  --defaults-file=/home/summerxwu/play_ground/kunlunrun/8001/data/8001/my_8001.cnf --no-server-version-check --backup --target-dir=./data/backup-cluster1-1639207032/xtrabackup_base --user=root --password=root"
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	sh.Run()
	time.Sleep(4 * time.Second)
	fmt.Println(sh.Stdout())
	fmt.Println(sh.Stderr())
	os.Exit(-1)
}
