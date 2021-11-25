package logger

import (
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	"os"
	_ "os"
	"path/filepath"
	"time"
)

var (
	Logger        *logrus.Logger
	prefixLogName string = ""
	Directory     string = ""
	MaxHour       int    = 7 * 24
	RotationHour  int    = 24
	RotationSize  int64  = 500 * 1024 * 1024 // 500M
)

func InitLogger() {

	Logger = logrus.New()

	if len(prefixLogName) == 0 {
		prefixLogName = "sys_" + filepath.Base(os.Args[0])
	}
	if len(Directory) == 0 {
		Directory = "../log"
	}
	var path = Directory + "/" + prefixLogName + ".%Y-%m-%d.log"

	ioptr, _ := rotatelogs.New(path,
		rotatelogs.WithMaxAge(time.Duration(MaxHour)*time.Hour),
		rotatelogs.WithRotationTime(time.Duration(RotationHour)*time.Hour),
		rotatelogs.WithRotationSize(RotationSize))
	Logger.SetOutput(ioptr)
	Logger.SetLevel(logrus.TraceLevel)
	Logger.SetFormatter(&logrus.TextFormatter{
		DisableColors:   true,
		TimestampFormat: "2021-01-02 15:03:04"})

}

func Debug(format string, args ...interface{}) {
	Logger.Debugf(format, args)
}
func Error(format string, args ...interface{}) {
	Logger.Errorf(format, args)
}
func Fatal(format string, args ...interface{}) {
	Logger.Fatalf(format, args)
}
func Info(format string, args ...interface{}) {
	Logger.Infof(format, args)
}
func Warn(format string, args ...interface{}) {
	Logger.Warnf(format, args)
}
