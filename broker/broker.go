package broker

import (
	"context"
	"fmt"
	"github.com/paust-team/paustq/broker/internals"
	"github.com/paust-team/paustq/broker/service"
	"github.com/paust-team/paustq/broker/storage"
	"github.com/paust-team/paustq/common"
	"github.com/paust-team/paustq/log"
	"github.com/paust-team/paustq/message"
	"github.com/paust-team/paustq/zookeeper"
	"net"
	"os"
)

var (
	DefaultBrokerHomeDir = os.ExpandEnv("$HOME/.paustq")
	DefaultLogDir        = fmt.Sprintf("%s/log", DefaultBrokerHomeDir)
	DefaultDataDir       = fmt.Sprintf("%s/data", DefaultBrokerHomeDir)
	DefaultLogLevel      = logger.Info
)

type Broker struct {
	Port            uint16
	host            string
	listener        net.Listener
	streamService   *service.StreamService
	db              *storage.QRocksDB
	notifier        *internals.Notifier
	zkClient        *zookeeper.ZKClient
	logDir          string
	dataDir         string
	logger          *logger.QLogger
	cancelBrokerCtx context.CancelFunc
}

func NewBroker(zkAddr string) *Broker {

	notifier := internals.NewNotifier()
	l := logger.NewQLogger("Broker", DefaultLogLevel)
	zkClient := zookeeper.NewZKClient(zkAddr)

	return &Broker{
		Port:     common.DefaultBrokerPort,
		notifier: notifier,
		zkClient: zkClient,
		logDir:   DefaultLogDir,
		dataDir:  DefaultDataDir,
		logger:   l,
	}
}

func (b *Broker) WithPort(port uint16) *Broker {
	b.Port = port
	return b
}

func (b *Broker) WithLogDir(dir string) *Broker {
	b.logDir = dir
	return b
}

func (b *Broker) WithDataDir(dir string) *Broker {
	b.dataDir = dir
	return b
}

func (b *Broker) WithLogLevel(level logger.LogLevel) *Broker {
	b.logger.SetLogLevel(level)
	return b
}

func (b *Broker) Start() {
	brokerCtx, cancelFunc := context.WithCancel(context.Background())
	b.cancelBrokerCtx = cancelFunc

	if err := b.createDirs(); err != nil {
		b.logger.Fatal(err)
	}

	b.logger = b.logger.WithFile(b.logDir)

	if err := b.connectToRocksDB(); err != nil {
		b.logger.Fatalf("error occurred while connecting to rocksdb : %v", err)
	}
	b.logger.Info("connected to rocksdb")

	if err := b.setUpZookeeper(); err != nil {
		b.logger.Fatalf("error occurred while setting up zookeeper : %v", err)
	}
	b.logger.Info("connected to zookeeper")

	notiErrorCh := b.notifier.NotifyNews(brokerCtx)

	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:"+string(b.Port))
	if err != nil {
		b.logger.Fatalf("failed to resolve tcp address", err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		b.logger.Fatalf("fail to bind address to 1101 : %v", err)
	}
	b.listener = listener

	sessionCh, acceptErrCh := b.handleNewConnections(brokerCtx)
	b.streamService = service.NewStreamService(b.db, b.notifier, b.zkClient, b.host)
	brokerStoppableErrCh, sessionCloseableErrCh, sustainableErrCh := b.streamService.HandleNewSessions(brokerCtx, sessionCh)

	b.logger.Infof("start broker with port: %d", b.Port)

	for {
		select {
		case <-brokerCtx.Done():
			return
		case <-notiErrorCh:
			return
		case <-acceptErrCh:
			return
		case errMsg := <-brokerStoppableErrCh:
			if errMsg.Broadcastable {
				b.streamService.BroadcastMsg(message.NewErrorAckMsg(errMsg.Err.Code(), errMsg.Err.Error()))
			}
			return
		case errMsg := <-sessionCloseableErrCh:
			if errMsg.ClientVisible {
				errMsg.Session.Write(message.NewErrorAckMsg(errMsg.Err.Code(), errMsg.Err.Error()))
			}
			errMsg.CancelSession()
		case errMsg := <-sustainableErrCh:
			if errMsg.Broadcastable {
				b.streamService.BroadcastMsg(message.NewErrorAckMsg(errMsg.Err.Code(), errMsg.Err.Error()))
			} else if errMsg.ClientVisible {
				errMsg.Session.Write(message.NewErrorAckMsg(errMsg.Err.Code(), errMsg.Err.Error()))
			}
		}
	}
}

func (b *Broker) Stop() {
	b.listener.Close()
	b.db.Close()
	b.tearDownZookeeper()
	b.cancelBrokerCtx()
	b.logger.Info("broker stopped")
	b.logger.Close()
}

func (b *Broker) Clean() {
	b.logger.Info("clean broker")
	_ = b.db.Destroy()
	b.tearDownZookeeper()
	os.RemoveAll(b.logDir)
	os.RemoveAll(b.dataDir)
}

func (b *Broker) createDirs() error {
	if err := os.MkdirAll(b.dataDir, os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll(b.logDir, os.ModePerm); err != nil {
		return err
	}
	return nil
}

func (b *Broker) connectToRocksDB() error {
	db, err := storage.NewQRocksDB("shapleq-store", b.dataDir)
	if err != nil {
		return err
	}
	b.db = db
	return nil
}

func (b *Broker) setUpZookeeper() error {
	host, err := zookeeper.GetOutboundIP()
	if err != nil {
		b.logger.Error(err)
		return err
	}
	if !zookeeper.IsPublicIP(host) {
		b.logger.Warning("cannot attach to broker from external network")
	}

	b.host = host.String()
	b.zkClient = b.zkClient.WithLogger(b.logger)
	if err := b.zkClient.Connect(); err != nil {
		return err
	}

	if err := b.zkClient.CreatePathsIfNotExist(); err != nil {
		return err
	}

	if err := b.zkClient.AddBroker(b.host); err != nil {
		return err
	}

	return nil
}

func (b *Broker) tearDownZookeeper() {
	if err := b.zkClient.RemoveBroker(b.host); err != nil {
		b.logger.Error(err)
	}
	topics, _ := b.zkClient.GetTopics()
	for _, topic := range topics {
		if err := b.zkClient.RemoveTopicBroker(topic, b.host); err != nil {
			b.logger.Error(err)
		}
	}
	b.zkClient.Close()
}

func (b *Broker) handleNewConnections(ctx context.Context) (<-chan *internals.Session, <-chan error) {
	sessionCh := make(chan *internals.Session)
	errCh := make(chan error)

	go func() {
		defer close(sessionCh)
		defer close(errCh)
		for {
			conn, err := b.listener.Accept()
			if err != nil {
				if ne, ok := err.(net.Error); ok {
					if ne.Temporary() {
						b.logger.Infof("temporary error occurred while accepting new connections : %v", err)
						continue
					}
				}
				b.logger.Errorf("error occured while accepting new connections : %v", err)
				errCh <- err
				return
			}

			select {
			case sessionCh <- internals.NewSession(conn):
				b.logger.Infof("new connection created")
			case <-ctx.Done():
				return
			}
		}
	}()

	return sessionCh, errCh
}
