/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

package sshUtil

import (
	"bufio"
	"fmt"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
	"time"
)

func Sftp_connect(user, password, host string, port int) (*sftp.Client, error) {
	var (
		auth         []ssh.AuthMethod
		addr         string
		clientConfig *ssh.ClientConfig
		sshClient    *ssh.Client
		sftpClient   *sftp.Client
		err          error
	)
	// get auth method
	auth = make([]ssh.AuthMethod, 0)
	auth = append(auth, ssh.Password(password))

	clientConfig = &ssh.ClientConfig{
		User:            user,
		Auth:            auth,
		Timeout:         30 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// connet to ssh
	addr = fmt.Sprintf("%s:%d", host, port)

	if sshClient, err = ssh.Dial("tcp", addr, clientConfig); err != nil {
		return nil, err
	}

	// create sftp client
	if sftpClient, err = sftp.NewClient(sshClient); err != nil {
		return nil, err
	}

	return sftpClient, nil
}

func Push_to_remote(sftp *sftp.Client, remoteFile string, localFile string) {
	srcFile, err := os.Open(localFile)
	if err != nil {
		fmt.Println(err)
	}
	defer func(srcFile *os.File) {
		err := srcFile.Close()
		if err != nil {
			// TODO
		}
	}(srcFile)

	dstFile, err := sftp.Create(remoteFile)
	if err != nil {
		fmt.Println(err)
	}
	br := bufio.NewReader(srcFile)
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		var _, err = dstFile.Write(a)
		if err != nil {
			fmt.Printf("write to remote faild:%s", err)
		}
		rt := []byte("\n")
		dstFile.Write(rt)
	}
	fmt.Println("copy file to remote server finished")

}
