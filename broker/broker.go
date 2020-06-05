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
	"github.com/paust-team/paustq/network"
	"github.com/paust-team/paustq/pqerror"
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
	sessionMgr      *internals.SessionManager
	txService   	*service.TransactionService
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

	b.sessionMgr = internals.NewSessionManager()
	sessionAndContextCh, acceptErrCh := b.handleNewConnections(brokerCtx)
	//Need to implement transaction service
	txEventStreamCh, stEventStreamCh, sessionErrCh := b.generateEventStreams(sessionAndContextCh)

	b.streamService = service.NewStreamService(b.db, b.notifier, b.zkClient, b.host)
	b.txService = service.NewTransactionService(b.db, b.zkClient)

	sessionErrCh = pqerror.MergeSessionErrors(sessionErrCh, b.streamService.HandleEventStreams(brokerCtx, stEventStreamCh))
	txErrCh := b.txService.HandleEventStreams(brokerCtx, txEventStreamCh)

	b.logger.Infof("start broker with port: %d", b.Port)

	for {
		select {
		case <-brokerCtx.Done():
			return
		case err := <- txErrCh:
			b.logger.Errorf("error occurred on transaction service: %s", err)
			return
		case <-notiErrorCh:
			return
		case <-acceptErrCh:
			return
		case sessionErr := <-sessionErrCh:
			pqErr, ok := sessionErr.Err.(pqerror.PQError)
			if !ok {
				b.logger.Errorf("unhandled error occurred : %v", sessionErr.Err)
				return
			}
			b.logger.Errorf("error occurred from session : %v", pqErr)

			switch pqErr.(type) {
			case pqerror.IsClientVisible:
				sessionErr.Session.Write(message.NewErrorAckMsg(pqErr.Code(), pqErr.Error()))
			case pqerror.IsBroadcastable:
				b.sessionMgr.BroadcastMsg(message.NewErrorAckMsg(pqErr.Code(), pqErr.Error()))
			default:
			}

			switch pqErr.(type) {
			case pqerror.IsBrokerStoppable:
				return
			case pqerror.IsSessionCloseable:
				sessionErr.CancelSession()
			default:
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
				b.logger.Errorf("error occurred while accepting new connections : %v", err)
				errCh <- err
				return
			}

			sessionCtx, cancelSession := context.WithCancel(brokerCtx)

			select {
			case sessionCtxCh <- SessionAndContext{internals.NewSession(conn), sessionCtx, cancelSession}:

				b.logger.Infof("new connection created")
			case <-brokerCtx.Done():
				return
			}
		}
	}()

	return sessionCtxCh, errCh
}

type EventStream struct {
	Session       *internals.Session
	MsgCh         <-chan *message.QMessage
	Ctx           context.Context
	CancelSession context.CancelFunc
}

func (b *Broker) generateEventStreams(scCh <-chan SessionAndContext) (<-chan EventStream, <-chan EventStream, <-chan pqerror.SessionError) {
	transactionalEvents := make(chan EventStream)
	streamingEvents := make(chan EventStream)
	sessionErrCh := make(chan pqerror.SessionError)

	go func() {
		defer close(transactionalEvents)
		defer close(streamingEvents)
		defer close(sessionErrCh)

		for sc := range scCh {
			txMsgCh := make(chan *message.QMessage)
			streamMsgCh := make(chan *message.QMessage)
			go func() {
				defer close(txMsgCh)
				defer close(streamMsgCh)

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
						if msg.Type() == network.TRANSACTION {
							txMsgCh <- msg
						} else if msg.Type() == network.STREAM {
							streamMsgCh <- msg
						}
					case err := <-errCh:
						pqErr, ok := err.(pqerror.PQError)
						if !ok {
							sessionErrCh <- pqerror.SessionError{
								Err:           pqerror.UnhandledError{ErrStr: err.Error()},
								Session:       sc.session,
								CancelSession: sc.cancelSession}
						} else {
							sessionErrCh <- pqerror.SessionError{
								Err:           pqErr,
								Session:       sc.session,
								CancelSession: sc.cancelSession}
						}
					}
				}
			}()

			transactionalEvents <- EventStream{sc.session, txMsgCh, sc.ctx, sc.cancelSession}
			streamingEvents <- EventStream{sc.session, streamMsgCh, sc.ctx, sc.cancelSession}
		}
	}()

	return transactionalEvents, streamingEvents, sessionErrCh
}
