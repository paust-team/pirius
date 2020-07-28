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
	zkAddr  string
	conn    *zk.Conn
	timeout uint
	logger  *logger.QLogger
}

func NewZKClient(zkAddr string, timeout uint) *ZKClient {
	return &ZKClient{
		zkAddr:  zkAddr,
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

	z.conn, _, err = zk.Connect([]string{z.zkAddr}, time.Second*time.Duration(z.timeout), zk.WithLogger(z.logger))
	if err != nil {
		err = pqerror.ZKConnectionError{ZKAddr: z.zkAddr}
		z.logger.Error(err)
		return err
	}
	return nil
}

func (z *ZKClient) Close() {
	z.conn.Close()
}

func (z *ZKClient) CreatePathsIfNotExist() error {
	paths := []constants.ZKPath{constants.SHAPLEQ, constants.BROKERS, constants.TOPICS, constants.TOPIC_BROKERS}
	for _, path := range paths {
		err := z.createPathIfNotExists(path)
		if err != nil {
			z.logger.Error(err)
			return err
		}
	}
	return nil
}

func (z *ZKClient) createPathIfNotExists(path constants.ZKPath) error {
	_, err := z.conn.Create(path.String(), []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func topicPath(topic string) string {
	return constants.TOPICS.String() + "/" + topic
}

func topicLockPath(topic string) string {
	return fmt.Sprintf("/topics-%s-lock", topic)
}

func (z *ZKClient) AddTopic(topic string, topicMeta *internals.TopicMeta) error {
	tLock := zk.NewLock(z.conn, constants.TOPICS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TOPICS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Create(topicPath(topic), topicMeta.Data(), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if err == zk.ErrNodeExists {
			err = pqerror.ZKTargetAlreadyExistsError{Target: topicPath(topic)}
		} else {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
		z.logger.Error(err)
		return err
	}
	return nil
}

func (z *ZKClient) GetTopic(topic string) (*internals.TopicMeta, error) {
	tLock := zk.NewLock(z.conn, constants.TOPICS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TOPICS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	topicBytes, _, err := z.conn.Get(topicPath(topic))
	if err != nil {
		if err == zk.ErrNodeExists {
			err = pqerror.ZKTargetAlreadyExistsError{Target: topicPath(topic)}
		} else {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
		z.logger.Error(err)
		return nil, err
	}

	return internals.NewTopicMeta(topicBytes), nil
}

func (z *ZKClient) GetTopics() ([]string, error) {
	tLock := zk.NewLock(z.conn, constants.TOPICS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TOPICS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	topics, _, err := z.conn.Children(constants.TOPICS.String())
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
	tLock := zk.NewLock(z.conn, constants.TOPICS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TOPICS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	if err = z.conn.Delete(topicPath(topic), -1); err != nil {
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

	bLock := zk.NewLock(z.conn, constants.BROKERS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.BROKERS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(constants.BROKERS.String(), buffer.Bytes(), -1)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z *ZKClient) GetBrokers() ([]string, error) {
	bLock := zk.NewLock(z.conn, constants.BROKERS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err := bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.BROKERS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	brokersBytes, _, err := z.conn.Get(constants.BROKERS.String())
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

	bLock := zk.NewLock(z.conn, constants.BROKERS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.BROKERS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(constants.BROKERS.String(), buffer.Bytes(), -1)
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
			err = pqerror.ZKTargetAlreadyExistsError{Target: server}
			z.logger.Error(err)
			return err
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

	tLocks := zk.NewLock(z.conn, constants.TOPIC_BROKERS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err = tLocks.Lock()
	defer tLocks.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TOPIC_BROKERS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(constants.TOPIC_BROKERS.String(), buffer.Bytes(), -1)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z *ZKClient) GetTopicBrokers(topic string) ([]string, error) {
	tLock := zk.NewLock(z.conn, constants.TOPIC_BROKERS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TOPIC_BROKERS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	brokersBytes, _, err := z.conn.Get(constants.TOPIC_BROKERS.String())
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

	tLock := zk.NewLock(z.conn, constants.TOPIC_BROKERS_LOCK.String(), zk.WorldACL(zk.PermAll))
	err = tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: constants.TOPIC_BROKERS_LOCK.String(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(constants.TOPIC_BROKERS.String(), buffer.Bytes(), -1)
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
			z.conn.Delete(topicPath(topic), -1)
		}
	}

	err = z.conn.Delete(constants.TOPICS.String(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.TOPICS.String(), err)
	}
	err = z.conn.Delete(constants.BROKERS.String(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.BROKERS.String(), err)
	}
	err = z.conn.Delete(constants.TOPIC_BROKERS.String(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.TOPIC_BROKERS.String(), err)
	}
	err = z.conn.Delete(constants.BROKERS_LOCK.String(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.BROKERS_LOCK.String(), err)
	}
	err = z.conn.Delete(constants.TOPICS_LOCK.String(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.TOPICS_LOCK.String(), err)
	}
	err = z.conn.Delete(constants.TOPIC_BROKERS_LOCK.String(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.TOPIC_BROKERS_LOCK.String(), err)
	}
	err = z.conn.Delete(constants.SHAPLEQ.String(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", constants.SHAPLEQ.String(), err)
	}
}
