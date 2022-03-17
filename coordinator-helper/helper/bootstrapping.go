package helper

import (
	"bytes"
	"github.com/paust-team/shapleq/coordinator"
	"github.com/paust-team/shapleq/coordinator-helper/constants"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"strings"
)

type BootstrappingHelper struct {
	client coordinator.Coordinator
	logger *logger.QLogger
}

func NewBootstrappingHelper(client coordinator.Coordinator) *BootstrappingHelper {
	return &BootstrappingHelper{client: client}
}

func (b *BootstrappingHelper) WithLogger(logger *logger.QLogger) *BootstrappingHelper {
	b.logger = logger
	return b
}

func (b BootstrappingHelper) GetBrokers() ([]string, error) {
	if brokersBytes, err := b.client.
		Get(constants.BrokersPath).
		Run(); err == nil {
		if len(brokersBytes) == 0 {
			if b.logger != nil {
				b.logger.Info("no broker exists")
			}
			return nil, nil
		}

		brokers := strings.Split(bytes.NewBuffer(brokersBytes).String(), ",")
		return brokers, nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.BrokerNotExistsError{}
	} else {
		return nil, err
	}
}

func (b BootstrappingHelper) AddBroker(hostName string) error {
	brokers, err := b.GetBrokers()
	if err != nil {
		return err
	}

	for _, broker := range brokers {
		if broker == hostName {
			if b.logger != nil {
				b.logger.Info("broker already exists")
			}
			return nil
		}
	}

	brokers = append(brokers, hostName)
	if err = b.client.
		Set(constants.BrokersPath, []byte(strings.Join(brokers, ","))).
		WithLock(constants.BrokersLockPath).
		Run(); err != nil {
		return err
	}

	return nil
}

func (b BootstrappingHelper) RemoveBroker(hostName string) error {
	brokers, err := b.GetBrokers()
	if err != nil {
		return err
	}

	found := false
	for i, broker := range brokers {
		if broker == hostName {
			brokers = append(brokers[:i], brokers[i+1:]...)
			found = true
			break
		}
	}

	if found == false {
		return pqerror.ZKNothingToRemoveError{}
	}

	if err = b.client.
		Set(constants.BrokersPath, []byte(strings.Join(brokers, ","))).
		WithLock(constants.BrokersLockPath).
		Run(); err != nil {
		return err
	}

	return nil
}

func (b BootstrappingHelper) GetBrokersOfTopic(topicName string, fragmentId uint32) ([]string, error) {
	if brokersBytes, err := b.client.
		Get(constants.GetTopicFragmentBrokerBasePath(topicName, fragmentId)).
		Run(); err == nil {
		if len(brokersBytes) == 0 {
			if b.logger != nil {
				b.logger.Info("no broker exists")
			}
			return nil, nil
		}

		topicFragmentBrokers := strings.Split(bytes.NewBuffer(brokersBytes).String(), ",")
		return topicFragmentBrokers, nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.TopicFragmentBrokerNotExistsError{Topic: topicName, FragmentId: fragmentId}
	} else {
		return nil, err
	}
}

func (b BootstrappingHelper) AddBrokerForTopic(topicName string, fragmentId uint32, hostName string) error {
	topicBrokers, err := b.GetBrokersOfTopic(topicName, fragmentId)
	if err != nil {
		return err
	}

	for _, topicBroker := range topicBrokers {
		if topicBroker == hostName {
			if b.logger != nil {
				b.logger.Info(pqerror.ZKTargetAlreadyExistsError{Target: hostName})
			}
			return nil
		}
	}

	topicBrokers = append(topicBrokers, hostName)
	if err = b.client.
		Set(constants.GetTopicFragmentBrokerBasePath(topicName, fragmentId), []byte(strings.Join(topicBrokers, ","))).
		WithLock(constants.GetTopicFragmentBrokerLockPath(topicName, fragmentId)).
		Run(); err != nil {
		return err
	}

	return nil
}

func (b BootstrappingHelper) RemoveBrokerOfTopic(topicName string, fragmentId uint32, hostName string) error {
	topicFragmentBrokers, err := b.GetBrokersOfTopic(topicName, fragmentId)
	if err != nil {
		return err
	}

	found := false
	for i, broker := range topicFragmentBrokers {
		if broker == hostName {
			topicFragmentBrokers = append(topicFragmentBrokers[:i], topicFragmentBrokers[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		err = pqerror.ZKNothingToRemoveError{}
		if b.logger != nil {
			b.logger.Error(err)
		}
		return err
	}

	if err = b.client.
		Set(constants.GetTopicFragmentBrokerBasePath(topicName, fragmentId), []byte(strings.Join(topicFragmentBrokers, ","))).
		WithLock(constants.GetTopicFragmentBrokerLockPath(topicName, fragmentId)).
		Run(); err != nil {
		return err
	}

	return nil
}
