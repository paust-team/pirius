package broker

import (
	"context"
	"fmt"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/service"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/common"
	"github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/paust-team/shapleq/zookeeper"
	"net"
	"os"
	"strconv"
	"sync"
)

type Broker struct {
	config          *config.BrokerConfig
	Port            uint
	host            string
	listener        net.Listener
	streamService   *service.StreamService
	sessionMgr      *internals.SessionManager
	txService       *service.TransactionService
	db              *storage.QRocksDB
	notifier        *internals.Notifier
	zkClient        *zookeeper.ZKClient
	logger          *logger.QLogger
	cancelBrokerCtx context.CancelFunc
	closed          bool
}

func NewBroker(config *config.BrokerConfig) *Broker {

	notifier := internals.NewNotifier()
	l := logger.NewQLogger("Broker", config.LogLevel())
	zkClient := zookeeper.NewZKClient(config.ZKAddr())

	return &Broker{
		config:   config,
		Port:     common.DefaultBrokerPort,
		notifier: notifier,
		zkClient: zkClient,
		logger:   l,
		closed:   false,
	}
}

func (b *Broker) Config() *config.BrokerConfig {
	return b.config
}

func (b *Broker) Start() {
	brokerCtx, cancelFunc := context.WithCancel(context.Background())
	b.cancelBrokerCtx = cancelFunc

	if err := b.createDirs(); err != nil {
		b.logger.Fatal(err)
	}

	b.logger = b.logger.WithFile(b.config.LogDir())

	if err := b.connectToRocksDB(); err != nil {
		b.logger.Fatalf("error occurred while connecting to rocksdb : %v", err)
	}
	b.logger.Info("connected to rocksdb")

	if err := b.setUpZookeeper(); err != nil {
		b.logger.Fatalf("error occurred while setting up zookeeper : %v", err)
	}
	b.logger.Info("connected to zookeeper")

	notiErrorCh := b.notifier.NotifyNews(brokerCtx)
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("0.0.0.0:%d", b.Port))
	if err != nil {
		b.logger.Fatalf("failed to resolve tcp address %s", err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		b.logger.Fatalf("fail to bind address to %d : %v", b.Port, err)
	}
	b.listener = listener

	b.sessionMgr = internals.NewSessionManager()
	sessionAndContextCh, acceptErrCh := b.handleNewConnections(brokerCtx)
	//Need to implement transaction service
	txEventStreamCh, stEventStreamCh, sessionErrCh := b.generateEventStreams(sessionAndContextCh)

	b.streamService = service.NewStreamService(b.db, b.notifier, b.zkClient, fmt.Sprintf("%s:%d", b.host, b.Port))
	b.txService = service.NewTransactionService(b.db, b.zkClient)

	sessionErrCh = pqerror.MergeErrors(sessionErrCh, b.streamService.HandleEventStreams(brokerCtx, stEventStreamCh))
	txErrCh := b.txService.HandleEventStreams(brokerCtx, txEventStreamCh)

	b.logger.Infof("start broker with port: %d", b.Port)

	for {
		select {
		case <-brokerCtx.Done():
			return
		case err := <-txErrCh:
			if err != nil {
				b.logger.Errorf("error occurred on transaction service: %s", err)
			}
			return
		case <-notiErrorCh:
			return
		case <-acceptErrCh:
			return
		case sessionErr := <-sessionErrCh:
			if sessionErr != nil {
				sessErr, ok := sessionErr.(internals.SessionError)
				if !ok {
					b.logger.Errorf("unhandled error occurred : %v", sessionErr.Error())
					return
				}
				b.logger.Errorf("error occurred from session : %v", sessErr)

				switch sessErr.PQError.(type) {
				case pqerror.IsClientVisible:
					sessErr.Session.Write(message.NewErrorAckMsg(sessErr.Code(), sessErr.Error()))
				case pqerror.IsBroadcastable:
					b.sessionMgr.BroadcastMsg(message.NewErrorAckMsg(sessErr.Code(), sessErr.Error()))
				default:
				}

				switch sessErr.PQError.(type) {
				case pqerror.IsBrokerStoppable:
					return
				case pqerror.IsSessionCloseable:
					sessErr.CancelSession()
				default:
				}
			}
		}
	}
}

func (b *Broker) Stop() {
	b.closed = true
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
	os.RemoveAll(b.config.LogDir())
	os.RemoveAll(b.config.DataDir())
}

func (b *Broker) createDirs() error {
	if err := os.MkdirAll(b.config.DataDir(), os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll(b.config.LogDir(), os.ModePerm); err != nil {
		return err
	}
	return nil
}

func (b *Broker) connectToRocksDB() error {
	db, err := storage.NewQRocksDB("shapleq-store", b.config.DataDir())
	if err != nil {
		return err
	}
	b.db = db
	return nil
}

func (b *Broker) setUpZookeeper() error {
	host, err := network.GetOutboundIP()
	if err != nil {
		b.logger.Error(err)
		return err
	}

	if !network.IsPublicIP(host) {
		b.logger.Warning("cannot attach to broker from external network")
	}

	b.host = host.String()
	b.zkClient = b.zkClient.WithLogger(b.logger).WithTimeout(b.config.ZKTimeout())
	if err := b.zkClient.Connect(); err != nil {
		return err
	}

	if err := b.zkClient.CreatePathsIfNotExist(); err != nil {
		return err
	}

	if err := b.zkClient.AddBroker(host.String() + ":" + strconv.Itoa(int(b.Port))); err != nil {
		return err
	}

	return nil
}

func (b *Broker) tearDownZookeeper() {
	_ = b.zkClient.RemoveBroker(b.host)
	topics, _ := b.zkClient.GetTopics()
	for _, topic := range topics {
		_ = b.zkClient.RemoveTopicBroker(topic, b.host)
	}
	b.zkClient.Close()
}

type SessionAndContext struct {
	session       *internals.Session
	ctx           context.Context
	cancelSession context.CancelFunc
}

func (b *Broker) handleNewConnections(brokerCtx context.Context) (<-chan SessionAndContext, <-chan error) {
	sessionCtxCh := make(chan SessionAndContext)
	errCh := make(chan error)

	go func() {
		defer close(sessionCtxCh)
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
				if !b.closed {
					b.logger.Errorf("error occurred while accepting new connections : %v", err)
					errCh <- err
				}

				return
			}

			sessionCtx, cancelSession := context.WithCancel(brokerCtx)

			select {
			case sessionCtxCh <- SessionAndContext{internals.NewSession(conn), sessionCtx, cancelSession}:

				b.logger.Info("new connection created")
			case <-brokerCtx.Done():
				return
			}
		}
	}()

	return sessionCtxCh, errCh
}

func (b *Broker) generateEventStreams(scCh <-chan SessionAndContext) (<-chan internals.EventStream, <-chan internals.EventStream, <-chan error) {
	transactionalEvents := make(chan internals.EventStream)
	streamingEvents := make(chan internals.EventStream)
	sessionErrCh := make(chan error)
	wg := sync.WaitGroup{}

	go func() {
		defer close(transactionalEvents)
		defer close(streamingEvents)
		defer close(sessionErrCh)
		defer wg.Wait()

		for sc := range scCh {
			txMsgCh := make(chan *message.QMessage)
			streamMsgCh := make(chan *message.QMessage)
			wg.Add(1)
			go func() {
				defer close(txMsgCh)
				defer close(streamMsgCh)
				defer wg.Done()

				b.sessionMgr.AddSession(sc.session)
				defer b.sessionMgr.RemoveSession(sc.session)

				sc.session.Open()
				defer sc.session.Close()

				msgCh, errCh, err := sc.session.ContinuousRead(sc.ctx)
				if err != nil {
					return
				}

				for {
					select {
					case <-sc.ctx.Done():
						return
					case msg := <-msgCh:
						if msg != nil {
							if msg.Type() == message.TRANSACTION {
								txMsgCh <- msg
							} else if msg.Type() == message.STREAM {
								streamMsgCh <- msg
							}
						}

					case err := <-errCh:
						if err != nil {
							pqErr, ok := err.(pqerror.PQError)
							if !ok {
								sessionErrCh <- internals.SessionError{
									PQError:       pqerror.UnhandledError{ErrStr: err.Error()},
									Session:       sc.session,
									CancelSession: sc.cancelSession}
							} else {
								sessionErrCh <- internals.SessionError{
									PQError:       pqErr,
									Session:       sc.session,
									CancelSession: sc.cancelSession}
							}
						}
					}
				}
			}()

			transactionalEvents <- internals.EventStream{sc.session, txMsgCh, sc.ctx, sc.cancelSession}
			streamingEvents <- internals.EventStream{sc.session, streamMsgCh, sc.ctx, sc.cancelSession}
		}
	}()

	return transactionalEvents, streamingEvents, sessionErrCh
}
