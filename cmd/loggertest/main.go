package main

import (
	"zetta_util/util/logger"
)

func main() {
	logger.InitLogger()
	var testMsg string = "hello zetta"
	for true {
		logger.Debug("this is the debug info %s", testMsg)
		logger.Error("this is the error info %s", testMsg)
		logger.Info("this is the info info %s", testMsg)
		logger.Warn("this is the warn info %s", testMsg)

	}
}
