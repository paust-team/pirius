package cli

import (
	"fmt"
	"github.com/ghodss/yaml"
	coordinator_helper "github.com/paust-team/shapleq/coordinator-helper"
	"github.com/paust-team/shapleq/coordinator-helper/helper"
	logger "github.com/paust-team/shapleq/log"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
)

var (
	startTopicFilePath string
)

type topicInfo struct {
	Topic     string   `yaml:"topic"`
	Fragments []uint32 `yaml:"fragments"`
}
type startTopicInfo struct {
	Topics []topicInfo `yaml:"topics"`
}

func readTopicInfoYml(path string) *startTopicInfo {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		log.Panic(err)
	}

	p := &startTopicInfo{}
	err = yaml.Unmarshal(buf, p)
	if err != nil {
		log.Panic(err)
	}

	return p
}

func NewTopicCmd() *cobra.Command {

	var topicCmd = &cobra.Command{
		Use:   "topic",
		Short: "setup/remove topic of shapleq",
		Run: func(cmd *cobra.Command, args []string) {
			coordinatorWrapper := coordinator_helper.NewCoordinatorWrapper([]string{zkQuorum}, zkTimeout, 0, logger.NewQLogger("shapleq-test", logger.Debug))
			if err := coordinatorWrapper.Connect(); err != nil {
				log.Panic(err)
			}
			defer coordinatorWrapper.Close()
			if cmd.Flags().Changed("clear") {
				coordinatorWrapper.RemoveAllPath()
			} else { // setup
				startTopics := readTopicInfoYml(startTopicFilePath)
				fmt.Println("load start topics from file")
				if err := coordinatorWrapper.CreatePathsIfNotExist(); err != nil {
					log.Panic(err)
				}
				for _, topic := range startTopics.Topics {
					if err := coordinatorWrapper.AddTopicFrame(topic.Topic, helper.NewFrameForTopicFromValues("", 0, 0, 0)); err != nil {
						log.Panic(err)
					}
					for _, fragmentId := range topic.Fragments {
						if err := coordinatorWrapper.AddTopicFragmentFrame(topic.Topic, fragmentId, helper.NewFrameForFragmentFromValues(0, 0)); err != nil {
							log.Panic(err)
						}
					}
					fmt.Printf("----\nTopic: %s\n- Fragments: %+v\n", topic.Topic, topic.Fragments)
				}
			}
		},
	}
	topicCmd.Flags().StringVar(&zkQuorum, "zk-quorum", "127.0.0.1", "zookeeper quorum")
	topicCmd.Flags().UintVar(&zkTimeout, "zk-timeout", 0, "zookeeper timeout")
	topicCmd.Flags().BoolP("clear", "c", false, "DANGER: use this option only if you intend to reset zookeeper directory after broker is terminated")
	topicCmd.Flags().StringVar(&startTopicFilePath, "file", "topic.yml", "file path for start topics")

	return topicCmd
}
