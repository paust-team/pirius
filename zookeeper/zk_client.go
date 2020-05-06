package zookeeper

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
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
		log.Println("failed to connect zookeeper", err)
		return err
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
	ok, _, err  := z.conn.Exists(path.string())
	if err != nil {
		log.Println("failed to request zookeeper while checking if path exists", err)
		return err
	}

	if !ok {
		_, err = z.conn.Create(path.string(), []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists{
			log.Println("failed to request zookeeper while creating path ", path.string())
			return err
		}
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
		return err
	}
	ok, _, err  := z.conn.Exists(topicPath(topic))
	if err != nil {
		log.Println("failed to create topic path", err)
		return err
	}
	if ok {
		return nil
	}

	_, err = z.conn.Create(topicPath(topic), nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Println("failed to create topic path", err)
		return err
	}



	return nil
}

func (z *ZKClient) GetTopics() ([]string, error) {
	tLock := zk.NewLock(z.conn, TOPICS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()

	if err != nil {
		return nil, err
	}

	topics, _, err := z.conn.Children(TOPICS.string())
	if err != nil {
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
		return err
	}

	if err = z.conn.Delete(topicPath(topic), -1); err != nil {
		return err
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
		return err
	}

	bLock := zk.NewLock(z.conn, BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		return err
	}
	_, err = z.conn.Set(BROKERS.string(), buffer.Bytes(), -1)
	if err != nil {
		return err
	}
	return nil
}

func (z *ZKClient) GetBrokers() ([]string, error) {
	bLock := zk.NewLock(z.conn, BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err := bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		return nil, err
	}

	brokersBytes, _, err := z.conn.Get(BROKERS.string())
	if err != nil  {
		return nil, err
	}

	if len(brokersBytes) == 0  {
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	buffer.Write(brokersBytes)

	var brokers []string
	err = gob.NewDecoder(buffer).Decode(&brokers)
	if err != nil {
		return nil, err
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
		return errors.New("broker does not exist")
	}

	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(brokers)
	if err != nil {
		return err
	}

	bLock := zk.NewLock(z.conn, BROKERS_LOCK.string(), zk.WorldACL(zk.PermAll))
	err = bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		return err
	}

	_, err = z.conn.Set(BROKERS.string(), buffer.Bytes(), -1)
	if err != nil {
		return err
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
			log.Println("topic broker already exists")
			return nil
		}
	}

	topicBrokers = append(topicBrokers, server)
	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(topicBrokers)
	if err != nil {
		return err
	}

	tLocks := zk.NewLock(z.conn, topicLockPath(topic), zk.WorldACL(zk.PermAll))
	err = tLocks.Lock()
	defer tLocks.Unlock()
	if err != nil {
		return err
	}

	_, err = z.conn.Set(topicPath(topic), buffer.Bytes(), -1)
	if err != nil {
		return err
	}

	return nil
}

func (z *ZKClient) GetTopicBrokers(topic string) ([]string, error) {
	tLock := zk.NewLock(z.conn, topicLockPath(topic), zk.WorldACL(zk.PermAll))
	err := tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		return nil, err
	}

	brokersBytes, _, err := z.conn.Get(topicPath(topic))
	if err != nil {
		return nil, err
	}
	if len(brokersBytes) == 0  {
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	buffer.Write(brokersBytes)

	var brokers []string
	err = gob.NewDecoder(buffer).Decode(&brokers)
	if err != nil {
		return nil, err
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
		log.Println("there is no broker to delete")
		return nil
	}

	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(brokers)
	if err != nil {
		return err
	}

	tLock := zk.NewLock(z.conn, topicLockPath(topic), zk.WorldACL(zk.PermAll))
	err = tLock.Lock()
	defer tLock.Unlock()
	if err != nil {
		return err
	}

	_, err = z.conn.Set(topicPath(topic), buffer.Bytes(), -1)
	if err != nil {
		return err
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


