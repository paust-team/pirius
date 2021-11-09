package zookeeper

import (
	"bytes"
	"encoding/gob"
	"github.com/paust-team/shapleq/common"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/paust-team/shapleq/zookeeper/constants"
)

func encode(data []string) (*bytes.Buffer, error) {
	buffer := &bytes.Buffer{}
	if err := gob.NewEncoder(buffer).Encode(data); err != nil {
		return nil, err
	}
	return buffer, nil
}

func decode(bytesData []byte) ([]string, error) {
	buffer := &bytes.Buffer{}
	buffer.Write(bytesData)

	var result []string
	if err := gob.NewDecoder(buffer).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

type bootstrappingHelper struct {
	client *zkClientWrapper
	logger *logger.QLogger
}

func (b bootstrappingHelper) GetBrokers() ([]string, error) {
	if brokersBytes, err := b.client.Get(constants.BrokersLockPath, constants.BrokersPath); err == nil {
		if len(brokersBytes) == 0 {
			b.client.Logger().Info("no broker exists")
			return nil, nil
		}

		brokers, err := decode(brokersBytes)
		if err != nil {
			err = pqerror.ZKDecodeFailError{}
			b.client.Logger().Error(err)
			return nil, err
		}

		return brokers, nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.BrokerNotExistsError{}
	} else {
		return nil, err
	}
}

func (b bootstrappingHelper) AddBroker(hostName string) error {
	brokers, err := b.GetBrokers()
	if err != nil {
		return err
	}

	for _, broker := range brokers {
		if broker == hostName {
			b.client.Logger().Info("broker already exists")
			return nil
		}
	}

	brokers = append(brokers, hostName)
	buffer, err := encode(brokers)
	if err != nil {
		err = pqerror.ZKEncodeFailError{}
		b.client.Logger().Error(err)
		return err
	}

	if err = b.client.Set(constants.BrokersLockPath, constants.BrokersPath, buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (b bootstrappingHelper) RemoveBroker(hostName string) error {
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

	buffer, err := encode(brokers)
	if err != nil {
		err = pqerror.ZKEncodeFailError{}
		b.logger.Error(err)
		return err
	}

	if err = b.client.Set(constants.BrokersLockPath, constants.BrokersPath, buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (b bootstrappingHelper) GetTopicBrokers(topicName string) ([]string, error) {
	if brokersBytes, err := b.client.Get(constants.TopicsLockPath, GetTopicBrokerPath(topicName)); err == nil {
		if len(brokersBytes) == 0 {
			b.logger.Info("no broker exists")
			return nil, nil
		}

		topicBrokers, err := decode(brokersBytes)
		if err != nil {
			err = pqerror.ZKDecodeFailError{}
			b.logger.Error(err)
			return nil, err
		}

		return topicBrokers, nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.TopicBrokerNotExistsError{Topic: topicName}
	} else {
		return nil, err
	}
}

func (b bootstrappingHelper) AddTopicBroker(topicName string, hostName string) error {
	topicBrokers, err := b.GetTopicBrokers(topicName)
	if err != nil {
		return err
	}

	for _, topicBroker := range topicBrokers {
		if topicBroker == hostName {
			b.logger.Info(pqerror.ZKTargetAlreadyExistsError{Target: hostName})
			return nil
		}
	}

	topicBrokers = append(topicBrokers, hostName)
	buffer, err := encode(topicBrokers)
	if err != nil {
		err = pqerror.ZKEncodeFailError{}
		b.logger.Error(err)
		return err
	}

	if err = b.client.Set(constants.TopicsLockPath, GetTopicBrokerPath(topicName), buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (b bootstrappingHelper) RemoveTopicBroker(topicName string, hostName string) error {
	topicBrokers, err := b.GetTopicBrokers(topicName)
	if err != nil {
		return err
	}

	found := false
	for i, broker := range topicBrokers {
		if broker == hostName {
			topicBrokers = append(topicBrokers[:i], topicBrokers[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		err = pqerror.ZKNothingToRemoveError{}
		b.logger.Error(err)
		return err
	}

	buffer, err := encode(topicBrokers)
	if err != nil {
		err = pqerror.ZKEncodeFailError{}
		b.logger.Error(err)
		return err
	}

	if err = b.client.Set(constants.TopicsLockPath, GetTopicBrokerPath(topicName), buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

type topicManagingHelper struct {
	client *zkClientWrapper
	logger *logger.QLogger
}

func (t topicManagingHelper) AddTopic(topicName string, topicData *common.TopicData) error {
	if err := t.client.Create(constants.TopicsLockPath, GetTopicPath(topicName), topicData.Data()); err != nil {
		return err
	}
	if err := t.client.CreatePathIfNotExists(GetTopicBrokerPath(topicName)); err != nil {
		return err
	}

	return nil
}

func (t topicManagingHelper) GetTopicData(topicName string) (*common.TopicData, error) {
	if result, err := t.client.Get(constants.TopicsLockPath, GetTopicPath(topicName)); err == nil {
		return common.NewTopicData(result), nil
	} else if _, ok := err.(*pqerror.ZKNoNodeError); ok {
		return nil, pqerror.TopicNotExistError{Topic: topicName}
	} else {
		return nil, err
	}
}

func (t topicManagingHelper) GetTopics() ([]string, error) {
	if topics, err := t.client.Children(constants.TopicsLockPath, constants.TopicsPath); err != nil {
		return nil, err
	} else if len(topics) > 0 {
		return topics, nil
	} else {
		return nil, nil
	}
}

func (t topicManagingHelper) RemoveTopic(topicName string) error {
	if err := t.client.Delete(constants.TopicsLockPath, GetTopicBrokerPath(topicName)); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		t.logger.Error(err)
		return err
	}

	if err := t.client.Delete(constants.TopicsLockPath, GetTopicPath(topicName)); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		t.logger.Error(err)
		return err
	}

	return nil
}

func (t topicManagingHelper) RemoveTopicPaths() {
	if topics, err := t.GetTopics(); err == nil {
		if topics != nil {
			var deletePaths []string
			for _, topic := range topics {
				deletePaths = append(deletePaths, GetTopicBrokerPath(topic))
				deletePaths = append(deletePaths, GetTopicPath(topic))
			}
			t.client.DeleteAll(constants.TopicsLockPath, deletePaths)
		}
	}
}
