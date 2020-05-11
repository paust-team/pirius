package zookeeper

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/paust-team/paustq/pqerror"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"net"
	"time"
)

type ZKPath string
const (
	PAUSTQ       ZKPath = "/paustq"
	BROKERS      ZKPath = "/paustq/brokers"
	TOPICS       ZKPath = "/paustq/topics"
	BROKERS_LOCK ZKPath = "/brokers-lock"
	TOPICS_LOCK  ZKPath = "/topics-lock"
)

func (zp ZKPath) string() string {
	return string(zp)
}

type ZKClient struct {
	zkAddr string
	conn *zk.Conn
}

func NewZKClient(zkAddr string) *ZKClient {
	return &ZKClient{
		zkAddr: zkAddr,
		conn:   nil,
	}
}

func (z *ZKClient) Connect() error {
	var err error
	z.conn, _, err =  zk.Connect([]string{z.zkAddr,}, time.Second * 3)
	if err != nil {
		return pqerror.ZKConnectionError{ZKAddr:z.zkAddr}
	}
	return nil
}

func (z *ZKClient) Close() {
	z.conn.Close()
}

func (z *ZKClient) CreatePathsIfNotExist() error {
	paths := []ZKPath{PAUSTQ, BROKERS, TOPICS,}
	for _, path := range paths {
		err := z.createPathIfNotExists(path)
		if err != nil {
			return err
		}
	}
	return nil
}

func (z *ZKClient) createPathIfNotExists(path ZKPath) error {
	_, err := z.conn.Create(path.string(), []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists{
		log.Println("failed to request zookeeper while creating path ", path.string())
		return pqerror.ZKRequestError{ZKErrStr:err.Error()}
	}

	return nil
}

func topicPath(topic string) string {
	return TOPICS.string() + "/" + topic
}

func topicLockPath(topic string) string {
	return fmt.Sprintf("/topics-%s-lock", topic)
}

func (z *ZKClient) AddTopic(topic string) error {
	tLock := zk.NewLock(z.conn, TOPICS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		return pqerror.ZKLockFailError{LockPath:TOPICS_LOCK.string(), ZKErrStr:err.Error()}
	}

	_, err = z.conn.Create(topicPath(topic), nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if err == zk.ErrNodeExists {
			return pqerror.ZKTargetAlreadyExistsError{Target:topicPath(topic)}
		} else {
			return pqerror.ZKRequestError{ZKErrStr:err.Error()}
		}
	}
	return nil
}

func (z *ZKClient) GetTopics() ([]string, error) {
	tLock := zk.NewLock(z.conn, TOPICS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		return nil, pqerror.ZKLockFailError{LockPath:TOPICS_LOCK.string(), ZKErrStr:err.Error()}
	}

	topics, _, err := z.conn.Children(TOPICS.string())
	if err != nil {
		return nil, pqerror.ZKRequestError{ZKErrStr:err.Error()}
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
		return pqerror.ZKLockFailError{LockPath:TOPICS_LOCK.string(), ZKErrStr:err.Error()}
	}

	if err = z.conn.Delete(topicPath(topic), -1); err != nil {
		return pqerror.ZKRequestError{ZKErrStr:err.Error()}
	}
	return nil
}

func (z *ZKClient) AddBroker(server string) error {
	var brokers []string
	var err error

	brokers, err = z.GetBrokers()
	if err != nil {
		return err
	}

	for _, broker := range brokers {
		if broker == server {
			log.Println("broker already exists")
			return nil
		}
	}

	brokers = append(brokers, server)
	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(brokers)
	if err != nil {
		return pqerror.ZKEncodeFailError{}
	}

	bLock := zk.NewLock(z.conn, BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		return pqerror.ZKLockFailError{LockPath:BROKERS_LOCK.string(), ZKErrStr:err.Error()}
	}
	_, err = z.conn.Set(BROKERS.string(), buffer.Bytes(), -1)
	if err != nil {
		return pqerror.ZKRequestError{ZKErrStr:err.Error()}
	}
	return nil
}

func (z *ZKClient) GetBrokers() ([]string, error) {
	bLock := zk.NewLock(z.conn, BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		return nil, pqerror.ZKLockFailError{LockPath:BROKERS_LOCK.string(), ZKErrStr:err.Error()}
	}

	brokersBytes, _, err := z.conn.Get(BROKERS.string())
	if err != nil  {
		return nil, pqerror.ZKRequestError{ZKErrStr:err.Error()}
	}

	if len(brokersBytes) == 0  {
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	buffer.Write(brokersBytes)

	var brokers []string
	err = gob.NewDecoder(buffer).Decode(&brokers)
	if err != nil {
		return nil, pqerror.ZKDecodeFailError{}
	}

	return brokers, nil
}

func (z *ZKClient) RemoveBroker(server string) error {
	brokers, err := z.GetBrokers()
	if err != nil {
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
		return pqerror.ZKEncodeFailError{}
	}

	bLock := zk.NewLock(z.conn, BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		return pqerror.ZKLockFailError{LockPath:BROKERS_LOCK.string(), ZKErrStr:err.Error()}
	}

	_, err = z.conn.Set(BROKERS.string(), buffer.Bytes(), -1)
	if err != nil {
		return pqerror.ZKRequestError{ZKErrStr:err.Error()}
	}

	return nil
}

func (z *ZKClient) AddTopicBroker(topic string, server string) error {
	topicBrokers, err := z.GetTopicBrokers(topic)
	if err != nil {
		return err
	}

	for _, topicBroker := range topicBrokers {
		if topicBroker == server {
			return pqerror.ZKTargetAlreadyExistsError{Target:server}
		}
	}

	topicBrokers = append(topicBrokers, server)
	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(topicBrokers)
	if err != nil {
		return pqerror.ZKEncodeFailError{}
	}

	tLocks := zk.NewLock(z.conn, topicLockPath(topic), zk.WorldACL(zk.PermAll))
	err = tLocks.Lock()
	defer tLocks.Unlock()
	if err != nil {
		return pqerror.ZKLockFailError{LockPath:topicLockPath(topic), ZKErrStr:err.Error()}
	}

	_, err = z.conn.Set(topicPath(topic), buffer.Bytes(), -1)
	if err != nil {
		return pqerror.ZKRequestError{ZKErrStr:err.Error()}
	}

	return nil
}

func (z *ZKClient) GetTopicBrokers(topic string) ([]string, error) {
	tLock := zk.NewLock(z.conn, topicLockPath(topic), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		return nil, pqerror.ZKLockFailError{LockPath:topicLockPath(topic), ZKErrStr:err.Error()}
	}

	brokersBytes, _, err := z.conn.Get(topicPath(topic))
	if err != nil {
		return nil, pqerror.ZKRequestError{ZKErrStr:err.Error()}
	}
	if len(brokersBytes) == 0  {
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	buffer.Write(brokersBytes)

	var brokers []string
	err = gob.NewDecoder(buffer).Decode(&brokers)
	if err != nil {
		return nil, pqerror.ZKDecodeFailError{}
	}

	return brokers, nil
}

func (z *ZKClient) RemoveTopicBroker(topic string, server string) error {
	brokers, err := z.GetTopicBrokers(topic)
	if err != nil {
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
		return pqerror.ZKNothingToRemoveError{}
	}

	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(brokers)
	if err != nil {
		return pqerror.ZKEncodeFailError{}
	}

	tLock := zk.NewLock(z.conn, topicLockPath(topic), zk.WorldACL(zk.PermAll))
	err = tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		return pqerror.ZKLockFailError{LockPath:topicLockPath(topic), ZKErrStr:err.Error()}
	}

	_, err = z.conn.Set(topicPath(topic), buffer.Bytes(), -1)
	if err != nil {
		return pqerror.ZKRequestError{ZKErrStr:err.Error()}
	}

	return nil
}

// for testing
func (z *ZKClient) RemoveAllPath() {
	topics, err := z.GetTopics()
	if err != nil {
		return
	}
	if topics != nil {
		for _, topic := range topics {
			z.conn.Delete(topicPath(topic), -1)
		}
	}

	err = z.conn.Delete(TOPICS.string(), -1)
	if err != nil {
		log.Println("failed to delete path /paustq/topics ", err)
	}
	z.conn.Delete(BROKERS.string(), -1)
	if err != nil {
		log.Println("failed to delete path /paustq/brokers ", err)
	}

	z.conn.Delete(PAUSTQ.string(), -1)
	if err != nil {
		log.Println("failed to delete path /paustq ", err)
	}
}


func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func IsPublicIP(IP net.IP) bool {
	if IP.IsLoopback() || IP.IsLinkLocalMulticast() || IP.IsLinkLocalUnicast() {
		return false
	}
	if ip4 := IP.To4(); ip4 != nil {
		switch true {
		case ip4[0] == 10:
			return false
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			return false
		case ip4[0] == 192 && ip4[1] == 168:
			return false
		default:
			return true
		}
	}
	return false
}


