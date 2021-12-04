/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package commonUtil

import (
	"testing"
)

func TestUntar(t *testing.T) {
	tarball := "a"
	target := "b"
	err := Untar(tarball, target)
	if err != nil {
		t.Fatalf("%s", err)
	}
}
