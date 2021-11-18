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
