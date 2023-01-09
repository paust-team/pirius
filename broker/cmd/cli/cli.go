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
	existsProcess := "ps -ef | grep 'pirius-broker start' | grep -v grep | awk '{print $2, $3}'"
	out, err := exec.Command("bash", "-c", existsProcess).Output()
	if err != nil {
		return false, 0
	}
	outs := strings.Split(string(out), "\n")
	for _, ln := range outs {
		if len(ln) != 0 && !strings.Contains(ln, strconv.Itoa(parentPid)) {
			pid, err := strconv.Atoi(strings.Split(ln, " ")[0])
			if err != nil {
				return false, 0
			}
			return true, pid
		}
	}
	return false, 0
}

var brokerCmd = &cobra.Command{
	Use:   "pirius-broker [command] (flags)",
	Short: "Pirius Broker cli",
}

func Main() {

	brokerCmd.AddCommand(
		NewStartCmd(),
	)

	if err := brokerCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
