package shellRunner

import (
	"fmt"
	"github.com/rfyiamcool/go-shell"
	"strings"
	"zetta_util/util/logger"
)

type ShellRunner struct {
	Command   string
	Args      []string
	Sh        *shell.Cmd
	retStatus shell.Status
}

func DoCmdTest(cmd string) {
	sh := NewShellRunner(cmd, make([]string, 0))
	err := sh.Run()
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(sh.OutPut())

}

func NewShellRunner(cmd string, args []string) *ShellRunner {

	c := &ShellRunner{
		Command: cmd,
		Args:    args,
		Sh:      nil,
	}
	if len(c.Args) != 0 {
		//generate the command
		var cmdWithArgs string = fmt.Sprintf("%s ", c.Command)
		for _, ag := range c.Args {
			cmdWithArgs = cmdWithArgs + " " + ag
		}
		c.Sh = shell.NewCommand(cmdWithArgs)
	} else {
		c.Sh = shell.NewCommand(c.Command)
	}
	return c
}

func (s *ShellRunner) Run() error {
	//fmt.Printf("will run %s\n", s.Sh.Bash)
	err := s.Sh.Run()
	s.retStatus = s.Sh.Status
	if err != nil {
		logger.Log.Error(fmt.Sprintf("exec cmd: %s output: %s", s.Sh.Bash, s.OutPut()))
		return fmt.Errorf("exec cmd: %s output: %s", s.Sh.Bash, s.OutPut())
	}
	return nil
}

func (s *ShellRunner) OutPut() string {
	var out, err string
	if len(s.Sh.Status.Stdout) == 0 {
		out = "nil "
	} else {
		out = s.Sh.Status.Stdout
	}

	if len(s.Sh.Status.Stderr) == 0 {
		err = "nil "
	} else {
		err = s.Sh.Status.Stderr
	}

	for strings.HasSuffix(out, "\n") {
		out = strings.TrimSuffix(out, "\n")
	}
	return fmt.Sprintf("stdout:%s stderr:%s", out, err)
}

func (s *ShellRunner) Stdout() string {
	out := s.retStatus.Stdout
	for strings.HasSuffix(out, "\n") {
		out = strings.TrimSuffix(out, "\n")
	}
	return out
}

func (s *ShellRunner) Stderr() string {
	return s.retStatus.Stderr
}
func (s *ShellRunner) Status() int {
	return s.retStatus.ExitCode
}
