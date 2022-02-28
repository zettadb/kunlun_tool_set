/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package logger

import (
	"bytes"
	"fmt"
	"github.com/antonfisher/nested-logrus-formatter"
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	"os"
	_ "os"
	"path/filepath"
	"runtime"
	"time"
)

var (
	Log           *logrus.Logger
	prefixLogName string = ""
	Directory     string = ""
	MaxHour       int    = 7 * 24
	RotationHour  int    = 24
	RotationSize  int64  = 500 * 1024 * 1024 // 500M
)

func CallerFormat(entity *runtime.Frame) string {
	// output buffer
	b := &bytes.Buffer{}
	_, filename := filepath.Split(entity.File)
	_, function := filepath.Split(entity.Function)
	fmt.Fprintf(
		b,
		"  [caller - %s:%d %s -]",
		filename,
		entity.Line,
		function,
	)
	return b.String()
}

func SetUpLogger(dir string) {

	if len(dir) == 0 {
		Directory = "../log"
	} else {
		Directory = dir
	}

	var path = Directory + "/" + prefixLogName + ".%Y-%m-%d.log"

	ioptr, _ := rotatelogs.New(path,
		rotatelogs.WithMaxAge(time.Duration(MaxHour)*time.Hour),
		rotatelogs.WithRotationTime(time.Duration(RotationHour)*time.Hour),
		rotatelogs.WithRotationSize(RotationSize))
	Log.SetOutput(ioptr)
	Log.SetLevel(logrus.TraceLevel)
	Log.SetReportCaller(true)
	Log.SetFormatter(&formatter.Formatter{
		HideKeys:              true,
		NoColors:              true,
		CallerFirst:           false,
		TrimMessages:          true,
		TimestampFormat:       "2006-01-02 15:04:05.000",
		CustomCallerFormatter: CallerFormat,
	},
	)

}

func init() {
	Log = logrus.New()

	if len(prefixLogName) == 0 {
		prefixLogName = "sys_" + filepath.Base(os.Args[0])
	}
}
