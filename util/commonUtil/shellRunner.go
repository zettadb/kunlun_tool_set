package commonUtil

import (
	"fmt"
	"github.com/rfyiamcool/go-shell"
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
	//fmt.Println(sh.Stdout())
	//fmt.Println(sh.Stderr())
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
		fmt.Printf("running %s \nerr occour: %s", s.Sh.Bash, s.OutPut())
		return err
	}
	return nil
}

func (s *ShellRunner) OutPut() string {
	return fmt.Sprintf("exit code: %d, stdout: %s, stderr: %s",
		s.Sh.Status.ExitCode, s.Sh.Status.Stdout, s.Sh.Status.Stderr)
}

func (s *ShellRunner) Stdout() string {
	return s.retStatus.Stdout
}

func (s *ShellRunner) Stderr() string {
	return s.retStatus.Stderr
}
func (s *ShellRunner) Status() int {
	return s.retStatus.ExitCode
}
