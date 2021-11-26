package shellRunner

import (
	"testing"
	"zetta_util/util/logger"
)

func TestDoCmdTest(t *testing.T) {
	logger.InitLogger()
	DoCmdTest("la -a /")
}
