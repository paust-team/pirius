package zookeeper

import (
	"fmt"
	logger "github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/paust-team/shapleq/zookeeper/constants"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ZKQClient struct {
	*bootstrappingHelper
	*topicManagingHelper
	zkAddr  string
	timeout uint
	logger  *logger.QLogger
	client  *zkClientWrapper
}

func NewZKQClient(zkAddr string, timeout uint) *ZKQClient {
	logger := logger.NewQLogger("ZkClient", logger.Info)
	return &ZKQClient{
		zkAddr:  zkAddr,
		timeout: timeout,
		logger:  logger,
	}
}

func (z *ZKQClient) WithLogger(logger *logger.QLogger) *ZKQClient {
	z.logger.Inherit(logger)
	return z
}

func (z *ZKQClient) Connect() error {
	client, err := initZkClientWrapper([]string{z.zkAddr}, z.timeout, z.logger)
	if err != nil {
		return err
	}

	z.client = client
	z.bootstrappingHelper = &bootstrappingHelper{client: z.client, logger: z.logger}
	z.topicManagingHelper = &topicManagingHelper{client: z.client, logger: z.logger}

	return nil
}

func (z ZKQClient) Close() {
	z.client.Close()
}

func (z ZKQClient) CreatePathsIfNotExist() error {
	paths := []string{constants.ShapleQPath, constants.BrokersPath, constants.TopicsPath, constants.BrokersLockPath, constants.TopicsLockPath}
	for _, path := range paths {
		err := z.client.CreatePathIfNotExists(path)
		if err != nil {
			z.logger.Error(err)
			return err
		}
	}
	return nil
}

// for testing
func (z *ZKQClient) RemoveAllPath() {
	z.RemoveTopicPaths()
	deletePaths := []string{constants.TopicsPath, constants.BrokersPath, constants.BrokersLockPath,
		constants.TopicsLockPath, constants.BrokersLockPath, constants.ShapleQPath}

	z.client.DeleteAll("", deletePaths)
}

type zkClientWrapper struct {
	conn   *zk.Conn
	logger *logger.QLogger
}

func (z zkClientWrapper) Logger() *logger.QLogger {
	return z.logger
}

func initZkClientWrapper(addresses []string, timeout uint, logger *logger.QLogger) (*zkClientWrapper, error) {
	var err error

	conn, _, err := zk.Connect(addresses, time.Millisecond*time.Duration(timeout), zk.WithLogger(logger))
	if err != nil {
		err = pqerror.ZKConnectionError{ZKAddr: addresses[0]}
		logger.Error(err)
		return nil, err
	}

	return &zkClientWrapper{conn: conn, logger: logger}, nil
}

func (z zkClientWrapper) Close() {
	z.conn.Close()
}

func (z zkClientWrapper) CreatePathIfNotExists(path string) error {
	_, err := z.conn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z zkClientWrapper) Create(lockPath string, path string, value []byte) error {
	lock := zk.NewLock(z.conn, lockPath, zk.WorldACL(zk.PermAll))
	err := lock.Lock()
	defer lock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: lockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Create(path, value, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if err == zk.ErrNodeExists {
			z.logger.Info(pqerror.ZKTargetAlreadyExistsError{Target: path})
		} else {
			z.logger.Error(pqerror.ZKRequestError{ZKErrStr: err.Error()})
			return err
		}
	}
	return nil
}

func (z zkClientWrapper) Set(lockPath string, path string, value []byte) error {
	return z.SetWithVersion(lockPath, path, value, -1)
}

func (z zkClientWrapper) SetWithVersion(lockPath string, path string, value []byte, version int32) error {
	bLock := zk.NewLock(z.conn, lockPath, zk.WorldACL(zk.PermAll))
	err := bLock.Lock()
	defer bLock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: lockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	_, err = z.conn.Set(path, value, version)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}

	return nil
}

func (z zkClientWrapper) Get(lockPath string, path string) ([]byte, error) {
	value, _, err := z.GetWithVersion(lockPath, path)
	return value, err
}

func (z zkClientWrapper) GetWithVersion(lockPath string, path string) ([]byte, int32, error) {
	if len(lockPath) > 0 {
		lock := zk.NewLock(z.conn, lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		defer lock.Unlock()
		if err != nil {
			err = pqerror.ZKLockFailError{LockPath: lockPath, ZKErrStr: err.Error()}
			z.logger.Error(err)
			return nil, -1, err
		}
	}

	value, stats, err := z.conn.Get(path)
	if err != nil {
		if err == zk.ErrNoNode {
			err = pqerror.ZKNoNodeError{Path: path}
		} else {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
		z.logger.Error(err)
		return nil, -1, err
	}
	return value, stats.Version, nil
}

func (z zkClientWrapper) OptimisticSet(path string, postGet func([]byte, int32) ([]byte, int32)) error {
	value, stats, err := z.conn.Get(path)
	if err != nil {
		if err == zk.ErrNoNode {
			err = pqerror.ZKNoNodeError{Path: path}
		} else {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		}
		z.logger.Error(err)
		return err
	}

	data, version := postGet(value, stats.Version)
	_, err = z.conn.Set(path, data, version)
	if err != nil {
		if err == zk.ErrBadVersion {
			err = pqerror.ZKOperateError{ErrStr: "bad version. try again"}
			z.logger.Warning(err)
			return z.OptimisticSet(path, postGet)
		} else {
			err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
			z.logger.Error(err)
			return err
		}
	}

	return nil
}

func (z zkClientWrapper) Children(lockPath string, path string) ([]string, error) {
	lock := zk.NewLock(z.conn, lockPath, zk.WorldACL(zk.PermAll))
	err := lock.Lock()
	defer lock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: lockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	values, _, err := z.conn.Children(path)
	if err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return nil, err
	}

	return values, nil
}

func (z zkClientWrapper) Delete(lockPath string, path string) error {
	lock := zk.NewLock(z.conn, lockPath, zk.WorldACL(zk.PermAll))
	err := lock.Lock()
	defer lock.Unlock()
	if err != nil {
		err = pqerror.ZKLockFailError{LockPath: lockPath, ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}
	if err = z.conn.Delete(path, -1); err != nil {
		err = pqerror.ZKRequestError{ZKErrStr: err.Error()}
		z.logger.Error(err)
		return err
	}
	return nil
}

func (z zkClientWrapper) DeleteAll(lockPath string, paths []string) {
	if len(lockPath) > 0 {
		lock := zk.NewLock(z.conn, lockPath, zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		defer lock.Unlock()
		if err != nil {
			err = pqerror.ZKLockFailError{LockPath: lockPath, ZKErrStr: err.Error()}
			z.logger.Error(err)

		}
	}

	for _, path := range paths {
		z.conn.Delete(path, -1)
	}
}

func GetTopicPath(topic string) string {
	return fmt.Sprintf("%s/%s", constants.TopicsPath, topic)
}

func GetTopicBrokerPath(topic string) string {
	return fmt.Sprintf("%s/%s/brokers", constants.TopicsPath, topic)
}
