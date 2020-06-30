package cli

import (
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func checkRunningBrokerProcess() (bool, int) {

	parentPid := os.Getppid()
	existsProcess := "ps -ef | grep 'shapleq start' | grep -v grep | awk '{print $2, $3}'"
	out, err := exec.Command("bash", "-c", existsProcess).Output()
	if err != nil {
		return false, 0
	}
	outs := strings.Split(string(out), "\n")
	for _, ln := range outs {
		if len(ln) != 0 && !strings.Contains(ln, strconv.Itoa(parentPid)) {
			pid, err := strconv.Atoi(strings.Split(string(ln), " ")[0])
			if err != nil {
				return false, 0
			}
			return true, pid
		}
	}
	return false, 0
}

var paustQCmd = &cobra.Command{
	Use:   "shapleq [command] (flags)",
	Short: "PaustQ cli",
}

func Main() {

	paustQCmd.AddCommand(
		NewStartCmd(),
		NewStatusCmd(),
		NewStopCmd(),
	)

	if err := paustQCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
