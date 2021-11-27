package zookeeper

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/paust-team/shapleq/broker/internals"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/paust-team/shapleq/zookeeper/constants"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ZKClient struct {
	zkAddrs []string
	conn    *zk.Conn
	timeout uint
	logger  *logger.QLogger
}

func NewZKClient(zkAddrs []string, timeout uint) *ZKClient {
	return &ZKClient{
		zkAddrs: zkAddrs,
		timeout: timeout,
		conn:    nil,
		logger:  logger.NewQLogger("ZkClient", logger.Info),
	}
}

func (z *ZKClient) WithLogger(logger *logger.QLogger) *ZKClient {
	z.logger.Inherit(logger)
	return z
}

func (z *ZKClient) Connect() error {
	var err error

	z.conn, _, err = zk.Connect(z.zkAddrs, time.Millisecond*time.Duration(z.timeout), zk.WithLogger(z.logger))
	if err != nil {
		err = pqerror.ZKConnectionError{ZKAddrs: z.zkAddrs}
		z.logger.Error(err)
		return err
	}
	return nil
}

func (z *ZKClient) Close() {
	z.conn.Close()
}

func (z *ZKClient) CreatePathsIfNotExist() error {
	paths := []string{constants.ShapleQPath, constants.BrokersPath, constants.TopicsPath, constants.BrokersLockPath, constants.TopicsLockPath}
	for _, path := range paths {
		err := z.createPathIfNotExists(path)
		if err != nil {
			z.logger.Error(err)
			return err
		}
	}
	return nil
}

func (z *ZKClient) createPathIfNotExists(path string) error {
	_, err := z.conn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z *ZKClient) AddTopic(topic string, topicMeta *internals.TopicMeta) error {
	tLock := zk.NewLock(z.conn, constants.TopicsLockPath, zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TopicsLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Create(getTopicPath(topic), topicMeta.Data(), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if err == zk.ErrNodeExists {
			z.logger.Info(pqerror.ZKTargetAlreadyExistsError{Target: getTopicPath(topic)})
		} else {
			z.logger.Error(pqerror.ZKRequestError{ZKErrStr: err.Error()})
			return err
		}
	}

	if err := z.createPathIfNotExists(getTopicBrokerPath(topic)); err != nil {
		return err
	}

	return nil
}

func (z *ZKClient) GetTopic(topic string) (*internals.TopicMeta, error) {
	tLock := zk.NewLock(z.conn, constants.TopicsLockPath, zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TopicsLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	topicBytes, _, err := z.conn.Get(getTopicPath(topic))
	if err != nil {
		if err == zk.ErrNoNode {
			err = pqerror.TopicNotExistError{Topic: topic}
		} else {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
		z.logger.Error(err)
		return nil, err
	}

	return internals.NewTopicMeta(topicBytes), nil
}

func (z *ZKClient) GetTopics() ([]string, error) {
	tLock := zk.NewLock(z.conn, constants.TopicsLockPath, zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TopicsLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	topics, _, err := z.conn.Children(constants.TopicsPath)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	if len(topics) > 0 {
		return topics, nil
	}
	return nil, nil
}

func (z *ZKClient) RemoveTopic(topic string) error {
	tLock := zk.NewLock(z.conn, constants.TopicsLockPath, zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TopicsLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	if err = z.conn.Delete(getTopicBrokerPath(topic), -1); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	if err = z.conn.Delete(getTopicPath(topic), -1); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z *ZKClient) AddBroker(server string) error {
	var brokers []string
	var err error

	brokers, err = z.GetBrokers()
	if err != nil {
		z.logger.Error(err)
		return err
	}

	for _, broker := range brokers {
		if broker == server {
			z.logger.Info("broker already exists")
			return nil
		}
	}

	brokers = append(brokers, server)
	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(brokers)
	if err != nil {
		err = pqerror.ZKEncodeFailError{}
		z.logger.Error(err)
		return err
	}

	bLock := zk.NewLock(z.conn, constants.BrokersLockPath, zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.BrokersLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(constants.BrokersPath, buffer.Bytes(), -1)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z *ZKClient) GetBrokers() ([]string, error) {
	bLock := zk.NewLock(z.conn, constants.BrokersLockPath, zk.WorldACL(zk.PermAll))
	err := bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.BrokersLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	brokersBytes, _, err := z.conn.Get(constants.BrokersPath)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	if len(brokersBytes) == 0 {
		z.logger.Info("no broker exist")
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	buffer.Write(brokersBytes)

	var brokers []string
	err = gob.NewDecoder(buffer).Decode(&brokers)
	if err != nil {
		err = pqerror.ZKDecodeFailError{}
		z.logger.Error(err)
		return nil, err
	}

	return brokers, nil
}

func (z *ZKClient) RemoveBroker(server string) error {
	brokers, err := z.GetBrokers()
	if err != nil {
		z.logger.Error(err)
		return err
	}

	found := false
	for i, broker := range brokers {
		if broker == server {
			brokers = append(brokers[:i], brokers[i+1:]...)
			found = true
			break
		}
	}

	if found == false {
		return pqerror.ZKNothingToRemoveError{}
	}

	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(brokers)
	if err != nil {
		err = pqerror.ZKEncodeFailError{}
		z.logger.Error(err)
		return err
	}

	bLock := zk.NewLock(z.conn, constants.BrokersLockPath, zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.BrokersLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(constants.BrokersPath, buffer.Bytes(), -1)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z *ZKClient) AddTopicBroker(topic string, server string) error {

	topicBrokers, err := z.GetTopicBrokers(topic)
	if err != nil {
		z.logger.Error(err)
		return err
	}

	for _, topicBroker := range topicBrokers {
		if topicBroker == server {
			z.logger.Info(pqerror.ZKTargetAlreadyExistsError{Target: server})
			return nil
		}
	}

	topicBrokers = append(topicBrokers, server)
	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(topicBrokers)
	if err != nil {
		err = pqerror.ZKEncodeFailError{}
		z.logger.Error(err)
		return err
	}

	tLocks := zk.NewLock(z.conn, constants.TopicsLockPath, zk.WorldACL(zk.PermAll))
	err = tLocks.Lock()
	defer tLocks.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TopicsLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(getTopicBrokerPath(topic), buffer.Bytes(), -1)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z *ZKClient) GetTopicBrokers(topic string) ([]string, error) {
	tLock := zk.NewLock(z.conn, constants.TopicsLockPath, zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TopicsLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	brokersBytes, _, err := z.conn.Get(getTopicBrokerPath(topic))
	if err != nil {
		if err == zk.ErrNoNode {
			err = pqerror.TopicBrokerNotExistError{Topic: topic}
		} else {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
		z.logger.Error(err)
		return nil, err
	}

	if len(brokersBytes) == 0 {
		z.logger.Info("no broker exist in TopicBrokerPath")
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	buffer.Write(brokersBytes)

	var brokers []string
	err = gob.NewDecoder(buffer).Decode(&brokers)
	if err != nil {
		err = pqerror.ZKDecodeFailError{}
		z.logger.Error(err)
		return nil, err
	}

	return brokers, nil
}

func (z *ZKClient) RemoveTopicBroker(topic string, server string) error {
	brokers, err := z.GetTopicBrokers(topic)
	if err != nil {
		z.logger.Error(err)
		return err
	}

	found := false
	for i, broker := range brokers {
		if broker == server {
			brokers = append(brokers[:i], brokers[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		err = pqerror.ZKNothingToRemoveError{}
		z.logger.Error(err)
		return err
	}

	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(brokers)
	if err != nil {
		err = pqerror.ZKEncodeFailError{}
		z.logger.Error(err)
		return err
	}

	tLock := zk.NewLock(z.conn, constants.TopicsLockPath, zk.WorldACL(zk.PermAll))
	err = tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TopicsLockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(getTopicBrokerPath(topic), buffer.Bytes(), -1)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

// for testing
func (z *ZKClient) RemoveAllPath() {
	topics, err := z.GetTopics()
	if err != nil {
		z.logger.Error(err)
		return
	}
	if topics != nil {
		for _, topic := range topics {
			z.conn.Delete(getTopicBrokerPath(topic), -1)
			z.conn.Delete(getTopicPath(topic), -1)
		}
	}

	err = z.conn.Delete(constants.TopicsPath, -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.TopicsPath, err)
	}

	err = z.conn.Delete(constants.BrokersPath, -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.BrokersPath, err)
	}
	err = z.conn.Delete(constants.BrokersLockPath, -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.BrokersLockPath, err)
	}
	err = z.conn.Delete(constants.TopicsLockPath, -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.TopicsLockPath, err)
	}
	err = z.conn.Delete(constants.ShapleQPath, -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.ShapleQPath, err)
	}
}

func getTopicPath(topic string) string {
	return fmt.Sprintf("%s/%s", constants.TopicsPath, topic)
}

func getTopicBrokerPath(topic string) string {
	return fmt.Sprintf("%s/%s/brokers", constants.TopicsPath, topic)
}
