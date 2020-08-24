package broker

import (
	"context"
	"fmt"
	"github.com/paust-team/shapleq/broker/config"
	"github.com/paust-team/shapleq/broker/internals"
	"github.com/paust-team/shapleq/broker/service"
	"github.com/paust-team/shapleq/broker/storage"
	"github.com/paust-team/shapleq/log"
	"github.com/paust-team/shapleq/message"
	"github.com/paust-team/shapleq/network"
	"github.com/paust-team/shapleq/pqerror"
	"github.com/paust-team/shapleq/zookeeper"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"syscall"
)

type Broker struct {
	config          *config.BrokerConfig
	host            string
	listener        net.Listener
	sessionMgr      *internals.SessionManager
	db              *storage.QRocksDB
	topicMgr        *internals.TopicManager
	zkClient        *zookeeper.ZKClient
	logger          *logger.QLogger
	cancelBrokerCtx context.CancelFunc
	closed          bool
}

func NewBroker(config *config.BrokerConfig) *Broker {

	topicManager := internals.NewTopicManager()
	l := logger.NewQLogger("Broker", config.LogLevel())
	zkClient := zookeeper.NewZKClient(config.ZKAddr(), config.ZKTimeout())

	return &Broker{
		config:   config,
		topicMgr: topicManager,
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

	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("0.0.0.0:%d", b.config.Port()))
	if err != nil {
		b.logger.Fatalf("failed to resolve tcp address %s", err)
	}

	listenConfig := &net.ListenConfig{Control: reusePort}

	listener, err := listenConfig.Listen(brokerCtx, "tcp", tcpAddr.String())
	if err != nil {
		b.logger.Fatalf("fail to bind address to %d : %v", b.config.Port(), err)
	}
	b.listener = listener

	b.sessionMgr = internals.NewSessionManager()
	sessionAndContextCh, acceptErrCh := b.handleNewConnections(brokerCtx)

	sessionErrCh := b.generateEventStreams(brokerCtx, sessionAndContextCh)

	b.logger.Infof("start broker with port: %d", b.config.Port())

	for {
		select {
		case <-brokerCtx.Done():
			return

		case <-acceptErrCh:
			return
		case sessionErr := <-sessionErrCh:
			if sessionErr != nil {
				switch sessionErr.(type) {
				case internals.SessionError:
					sessErr := sessionErr.(internals.SessionError)

					b.logger.Infof("received sessionErr : %v", sessErr)

					switch sessErr.PQError.(type) {
					case pqerror.IsClientVisible:
						//sessErr.Session.Write(message.NewErrorAckMsg(sessErr.Code(), sessErr.Error()))
					case pqerror.IsBroadcastable:
						b.sessionMgr.BroadcastMsg(message.NewErrorAckMsg(sessErr.Code(), sessErr.Error()))
					default:
					}

					switch sessErr.PQError.(type) {
					case pqerror.IsBrokerStoppable:
						b.logger.Errorf("fatal error occurred : %v. stop broker", sessErr)
						return
					case pqerror.IsSessionCloseable:
						sessErr.CancelSession()
					default:
					}
				case internals.TransactionalError:
					txError := sessionErr.(internals.TransactionalError)
					b.logger.Errorf("error occurred from transactional service : %v", txError)

					switch txError.PQError.(type) {
					case pqerror.IsSessionCloseable:
						txError.CancelSession()
					default:
					}
				default:
					b.logger.Errorf("unhandled error occurred : %v. stop broker", sessionErr.Error())
					return
				}
			}
		default:
		}
		runtime.Gosched()
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
	b.zkClient = b.zkClient.WithLogger(b.logger)
	if err := b.zkClient.Connect(); err != nil {
		return err
	}

	if err := b.zkClient.CreatePathsIfNotExist(); err != nil {
		return err
	}

	if err := b.zkClient.AddBroker(host.String() + ":" + strconv.Itoa(int(b.config.Port()))); err != nil {
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
			case sessionCtxCh <- SessionAndContext{internals.NewSession(conn, b.config.Timeout()), sessionCtx, cancelSession}:

				b.logger.Info("new connection created")
			case <-brokerCtx.Done():
				return
			}
			runtime.Gosched()
		}
	}()

	return sessionCtxCh, errCh
}

func (b *Broker) generateEventStreams(ctx context.Context, scCh <-chan SessionAndContext) <-chan error {

	wg := sync.WaitGroup{}

	streamingEvents := make(chan internals.EventStream)
	transactionalEvents := make(chan internals.EventStream)

	streamService := service.NewStreamService(b.db, b.topicMgr, b.zkClient, fmt.Sprintf("%s:%d", b.host, b.config.Port()))
	transactionalService := service.NewTransactionService(b.db, b.zkClient)

	sessionErrCh := make(chan error)
	streamErrorCh := streamService.HandleEventStreams(ctx, streamingEvents)
	transactionalErrCh := transactionalService.HandleEventStreams(ctx, transactionalEvents)

	go func() {

		defer close(transactionalEvents)
		defer close(streamingEvents)
		defer close(sessionErrCh)
		defer wg.Wait()

		for sc := range scCh {
			sessAndCtx := sc
			txReadCh := make(chan *message.QMessage)
			streamReadCh := make(chan *message.QMessage)
			txWriteCh := make(chan *message.QMessage)
			streamWriteCh := make(chan *message.QMessage)

			b.sessionMgr.AddSession(sessAndCtx.session)
			sessAndCtx.session.Open()

			receiveCh, sendCh, errCh := sessAndCtx.session.ContinuousReadWrite()

			wg.Add(2)
			// When session context is done, close read channels for services(transactional, stream)
			// When read channels of services are closed, they will close write channel and terminated

			go func() {
				defer wg.Done()

				mu := sync.Mutex{}
				waitForCloseSession := false

				go func() {
					defer close(txReadCh)
					defer close(streamReadCh)
					for {
						select {
						case <-sessAndCtx.ctx.Done():
							mu.Lock()
							waitForCloseSession = true
							mu.Unlock()
							return
						default:
						}
						runtime.Gosched()
					}
				}()

				// This goroutine is for closing session after all services are terminated
				go func() {
					defer b.sessionMgr.RemoveSession(sessAndCtx.session)
					defer sessAndCtx.session.Close()
					defer wg.Done()

					serviceWg := sync.WaitGroup{}

					sendFromServiceCh := func(serviceWriteCh chan *message.QMessage) {
						serviceWg.Add(1)
						go func() {
							defer serviceWg.Done()
							for msg := range serviceWriteCh {
								sendCh <- msg
								runtime.Gosched()
							}
						}()
					}
					sendFromServiceCh(txWriteCh)
					sendFromServiceCh(streamWriteCh)

					serviceWg.Wait()
				}()

				for {
					select {
					case err, ok := <-errCh:
						if ok {
							pqErr, ok := err.(pqerror.PQError)
							if !ok {
								sessionErrCh <- internals.SessionError{
									PQError:       pqerror.UnhandledError{ErrStr: err.Error()},
									Session:       sessAndCtx.session,
									CancelSession: sessAndCtx.cancelSession}
							} else {
								sessionErrCh <- internals.SessionError{
									PQError:       pqErr,
									Session:       sessAndCtx.session,
									CancelSession: sessAndCtx.cancelSession}
							}
						}
					case msg, ok := <-receiveCh:
						if !ok {
							return
						}
						mu.Lock()
						if !waitForCloseSession {
							if msg.Type() == message.TRANSACTION {
								txReadCh <- msg
							} else if msg.Type() == message.STREAM {
								streamReadCh <- msg
							}
						}
						mu.Unlock()

					default:
					}
					runtime.Gosched()
				}
			}()

			transactionalEvents <- internals.EventStream{sessAndCtx.session, txReadCh, txWriteCh, sessAndCtx.ctx, sessAndCtx.cancelSession}
			streamingEvents <- internals.EventStream{sessAndCtx.session, streamReadCh, streamWriteCh, sessAndCtx.ctx, sessAndCtx.cancelSession}
			runtime.Gosched()
		}
	}()

	return pqerror.MergeErrors(sessionErrCh, streamErrorCh, transactionalErrCh)
}

func reusePort(network, address string, conn syscall.RawConn) error {
	return conn.Control(func(descriptor uintptr) {
		if err := syscall.SetsockoptInt(int(descriptor), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1); err != nil {
			panic(err)
		}
	})
}
