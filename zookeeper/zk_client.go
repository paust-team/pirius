package zookeeper

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/paust-team/shapleq/broker/internals"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ZKPath string

const (
	SHAPLEQ            ZKPath = "/shapleq"
	BROKERS            ZKPath = "/shapleq/brokers"
	TOPICS             ZKPath = "/shapleq/topics"
	TOPIC_BROKERS      ZKPath = "/shapleq/topic-brokers"
	BROKERS_LOCK       ZKPath = "/brokers-lock"
	TOPICS_LOCK        ZKPath = "/topics-lock"
	TOPIC_BROKERS_LOCK ZKPath = "/topic-brokers-lock"
)

func (zp ZKPath) string() string {
	return string(zp)
}

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
	paths := []ZKPath{SHAPLEQ, BROKERS, TOPICS, TOPIC_BROKERS}
	for _, path := range paths {
		err := z.createPathIfNotExists(path)
		if err != nil {
			z.logger.Error(err)
			return err
		}
	}
	return nil
}

func (z *ZKClient) createPathIfNotExists(path ZKPath) error {
	_, err := z.conn.Create(path.string(), []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func topicPath(topic string) string {
	return TOPICS.string() + "/" + topic
}

func topicLockPath(topic string) string {
	return fmt.Sprintf("/topics-%s-lock", topic)
}

func (z *ZKClient) AddTopic(topic string, topicMeta *internals.TopicMeta) error {
	tLock := zk.NewLock(z.conn, TOPICS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: TOPICS_LOCK.string(), ZKErrStr: err.Error()}
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
	tLock := zk.NewLock(z.conn, TOPICS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: TOPICS_LOCK.string(), ZKErrStr: err.Error()}
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
	tLock := zk.NewLock(z.conn, TOPICS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: TOPICS_LOCK.string(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	topics, _, err := z.conn.Children(TOPICS.string())
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
	tLock := zk.NewLock(z.conn, TOPICS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: TOPICS_LOCK.string(), ZKErrStr: err.Error()}
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

	bLock := zk.NewLock(z.conn, BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: BROKERS_LOCK.string(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(BROKERS.string(), buffer.Bytes(), -1)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z *ZKClient) GetBrokers() ([]string, error) {
	bLock := zk.NewLock(z.conn, BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: BROKERS_LOCK.string(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	brokersBytes, _, err := z.conn.Get(BROKERS.string())
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

	bLock := zk.NewLock(z.conn, BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: BROKERS_LOCK.string(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(BROKERS.string(), buffer.Bytes(), -1)
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

	tLocks := zk.NewLock(z.conn, TOPIC_BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err = tLocks.Lock()
	defer tLocks.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: TOPIC_BROKERS_LOCK.string(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(TOPIC_BROKERS.string(), buffer.Bytes(), -1)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z *ZKClient) GetTopicBrokers(topic string) ([]string, error) {
	tLock := zk.NewLock(z.conn, TOPIC_BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: TOPIC_BROKERS_LOCK.string(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	brokersBytes, _, err := z.conn.Get(TOPIC_BROKERS.string())
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

	tLock := zk.NewLock(z.conn, TOPIC_BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err = tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: TOPIC_BROKERS_LOCK.string(), ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(TOPIC_BROKERS.string(), buffer.Bytes(), -1)
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

	err = z.conn.Delete(TOPICS.string(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", TOPICS.string(), err)
	}
	err = z.conn.Delete(BROKERS.string(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", BROKERS.string(), err)
	}
	err = z.conn.Delete(TOPIC_BROKERS.string(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", TOPIC_BROKERS.string(), err)
	}
	err = z.conn.Delete(BROKERS_LOCK.string(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", BROKERS_LOCK.string(), err)
	}
	err = z.conn.Delete(TOPICS_LOCK.string(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", TOPICS_LOCK.string(), err)
	}
	err = z.conn.Delete(TOPIC_BROKERS_LOCK.string(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", TOPIC_BROKERS_LOCK.string(), err)
	}
	err = z.conn.Delete(SHAPLEQ.string(), -1)
	if err != nil {
		z.logger.Errorf("failed to delete path %s - %s ", SHAPLEQ.string(), err)
	}
}
