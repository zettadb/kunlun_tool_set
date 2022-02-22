/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package remoteFileOps

type FileOperateInterface interface {
	PushFileToRemote(srcFile string) error
	PullFileFromRemote(dstFile string) error
	RemoteLsFilesInfo(path string) []string
}

func PushToRemote(handler FileOperateInterface, srcFile string) error {
	return handler.PushFileToRemote(srcFile)
}

func PullFromRemote(handler FileOperateInterface, dstFilePath string) error {
	return handler.PullFileFromRemote(dstFilePath)

}
func RemoteLsInfo(handler FileOperateInterface, path string) []string {
	return handler.RemoteLsFilesInfo(path)
}
