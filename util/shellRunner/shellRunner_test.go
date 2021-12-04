/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package shellRunner

import (
	"testing"
)

func TestDoCmdTest(t *testing.T) {
	DoCmdTest("hadoop fs -ls /kunlun/backup/xtrabackup/cluster1/shard1 ")
}
