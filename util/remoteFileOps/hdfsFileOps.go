package remoteFileOps

import (
	"fmt"
	"zetta_util/util/configParse"
	"zetta_util/util/logger"
	"zetta_util/util/shellRunner"
)

type HdfsOperateType struct {
	Dir      string
	Filename string
}

func NewHdfsOperateType() *HdfsOperateType {
	return &HdfsOperateType{
		Dir:      "",
		Filename: ""}
}

func (h HdfsOperateType) PushFileToRemote(srcFile string) error {
	var cmd = ""

	if len(configParse.HdfsNameNode) != 0 {
		cmd = fmt.Sprintf("hadoop fs -fs %s -appendToFile %s %s/%s", configParse.HdfsNameNode, srcFile, h.Dir, h.Filename)
	} else {
		cmd = fmt.Sprintf("hadoop fs -appendToFile %s %s/%s", srcFile, h.Dir, h.Filename)
	}
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}
	logger.Log.Debug(fmt.Sprintf("run success: %s", cmd))
	return nil
}
func (h HdfsOperateType) PullFileFromRemote(dstFile string) error {
	var cmd = ""
	if len(configParse.HdfsNameNode) != 0 {
		cmd = fmt.Sprintf("hadoop fs -fs %s -get  %s %s", configParse.HdfsNameNode, fmt.Sprintf("%s/%s", h.Dir, h.Filename), dstFile)
	} else {
		cmd = fmt.Sprintf("hadoop fs -get  %s %s", fmt.Sprintf("%s/%s", h.Dir, h.Filename), dstFile)
	}
	sh := shellRunner.NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		return err
	}
	logger.Log.Debug(fmt.Sprintf("run success: %s", cmd))
	return nil

}

func (h HdfsOperateType) RemoteLsFilesInfo(path string) []string {
	return make([]string, 0)

}
