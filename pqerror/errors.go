package pqerror

import (
	"fmt"
)

// pipeline
type InvalidPipeTypeError struct {
	PipeName string
}

func (e InvalidPipeTypeError) Error() string {
	return fmt.Sprintf("Invalid pipe(%s) to add on pipeline", e.PipeName)
}

func (e InvalidPipeTypeError) Code() PQCode {
	return ErrInvalidPipeType
}

func (e InvalidPipeTypeError) IsSessionCloseable() {}

type PipeBuildFailError struct {
	PipeName string
}

func (e PipeBuildFailError) Error() string {
	return fmt.Sprintf("Invalid inputs to bulid pipe(%s)", e.PipeName)
}

func (e PipeBuildFailError) Code() PQCode {
	return ErrPipeBuildFail
}

func (e PipeBuildFailError) IsSessionCloseable() {}

type InvalidCaseFnCountError struct {
	NumCaseFn, CaseCount int
}

func (e InvalidCaseFnCountError) Error() string {
	return fmt.Sprintf("number of case functions(%d) does not match case count(%d)", e.NumCaseFn, e.CaseCount)
}

func (e InvalidCaseFnCountError) Code() PQCode {
	return ErrInvalidCaseFnCount
}

func (e InvalidCaseFnCountError) IsSessionCloseable() {}

type InvalidMsgTypeError struct{}

func (e InvalidMsgTypeError) Error() string {
	return "inStream data does not match any case functions"
}

func (e InvalidMsgTypeError) IsSessionCloseable() {}
func (e InvalidMsgTypeError) IsClientVisible()    {}
func (e InvalidMsgTypeError) Code() PQCode {
	return ErrInvalidMsgType
}

type InvalidStartOffsetError struct {
	Topic       string
	StartOffset uint64
	LastOffset  uint64
}

func (e InvalidStartOffsetError) Error() string {
	return fmt.Sprintf("requested start offset(%d) is greater than last offset(%d) of topic(%s)",
		e.StartOffset, e.LastOffset, e.Topic)
}

func (e InvalidStartOffsetError) IsSessionCloseable() {}
func (e InvalidStartOffsetError) IsClientVisible()    {}
func (e InvalidStartOffsetError) Code() PQCode {
	return ErrInvalidStartOffset
}

// session
type StateTransitionError struct {
	PrevState, NextState string
}

func (e StateTransitionError) Error() string {
	return fmt.Sprintf("invalid state transition - previous state: %s, next state : %s",
		e.PrevState, e.NextState)
}

func (e StateTransitionError) Code() PQCode {
	return ErrStateTransition
}

func (e StateTransitionError) IsSessionCloseable() {}

// zookeeper
type ZKConnectionError struct {
	ZKAddr string
}

func (e ZKConnectionError) Error() string {
	return "failed to connect zookeeper " + e.ZKAddr
}

func (e ZKConnectionError) IsBrokerStoppable() {}
func (e ZKConnectionError) IsBroadcastable()   {}
func (e ZKConnectionError) Code() PQCode {
	return ErrZKConnection
}

type ZKRequestError struct {
	ZKErrStr string
}

func (e ZKRequestError) Error() string {
	return "pqerror occurred during request to zookeeper : " + e.ZKErrStr
}

func (e ZKRequestError) Code() PQCode {
	return ErrZKRequest
}

func (e ZKRequestError) IsSessionCloseable() {}

type ZKTargetAlreadyExistsError struct {
	Target string
}

func (e ZKTargetAlreadyExistsError) Error() string {
	return fmt.Sprintf("target %s already exists", e.Target)
}

func (e ZKTargetAlreadyExistsError) Code() PQCode {
	return ErrZKTargetAlreadyExists
}

//func (e ZKTargetAlreadyExistsError) IsSessionCloseable() {}

type ZKLockFailError struct {
	LockPath string
	ZKErrStr string
}

func (e ZKLockFailError) Error() string {
	return fmt.Sprintf("locking path(%s) failed : %s", e.LockPath, e.ZKErrStr)
}

func (e ZKLockFailError) Code() PQCode {
	return ErrZKLockFail
}

func (e ZKLockFailError) IsSessionCloseable() {}

type ZKEncodeFailError struct{}

func (e ZKEncodeFailError) Error() string {
	return "failed to encode target to bytes"
}

func (e ZKEncodeFailError) Code() PQCode {
	return ErrZKEncodeFail
}

func (e ZKEncodeFailError) IsSessionCloseable() {}

type ZKDecodeFailError struct{}

func (e ZKDecodeFailError) Error() string {
	return "failed to decode bytes to target"
}

func (e ZKDecodeFailError) Code() PQCode {
	return ErrZKDecodeFail
}

func (e ZKDecodeFailError) IsSessionCloseable() {}

type ZKNothingToRemoveError struct{}

func (e ZKNothingToRemoveError) Error() string {
	return "target to remove from zookeeper does not exist"
}

func (e ZKNothingToRemoveError) Code() PQCode {
	return ErrZKNothingToRemove
}

func (e ZKNothingToRemoveError) IsSessionCloseable() {}

type ZKOperateError struct {
	ErrStr string
}

func (e ZKOperateError) Error() string {
	return "zk operate error : " + e.ErrStr
}

func (e ZKOperateError) Code() PQCode {
	return ErrZKOperate
}

// notifier
type TopicNotExistError struct {
	Topic string
}

func (e TopicNotExistError) Error() string {
	return fmt.Sprintf("topic(%s) does not exist", e.Topic)
}

func (e TopicNotExistError) Code() PQCode {
	return ErrTopicNotExist
}

func (e TopicNotExistError) IsBrokerStoppable() {}

func NewTopicNotExistError(topic string) TopicNotExistError {
	e := TopicNotExistError{Topic: topic}
	return e
}

// serialize / deserialize

type InvalidChecksumError struct{}

func (e InvalidChecksumError) Error() string {
	return "checksum of data body does not match specified checksum"
}

func (e InvalidChecksumError) Code() PQCode {
	return ErrInvalidChecksum
}

type NotEnoughBufferError struct{}

func (e NotEnoughBufferError) Error() string {
	return "size of data to serialize is smaller than size of header"
}

func (e NotEnoughBufferError) Code() PQCode {
	return ErrNotEnoughBuffer
}

//socket
// May be retryable

type ReadTimeOutError struct{}

func (e ReadTimeOutError) Error() string {
	return "read timed out"
}

func (e ReadTimeOutError) Code() PQCode {
	return ErrReadTimeOut
}

type WriteTimeOutError struct{}

func (e WriteTimeOutError) Error() string {
	return "write timed out"
}

func (e WriteTimeOutError) Code() PQCode {
	return ErrWriteTimeOut
}

type SocketReadError struct {
	ErrStr string
}

func (e SocketReadError) Error() string {
	return fmt.Sprintf("error occurred while reading data from socket: %s", e.ErrStr)
}

func (e SocketReadError) Code() PQCode {
	return ErrSocketRead
}

// May be retryable
type SocketWriteError struct {
	ErrStr string
}

func (e SocketWriteError) Error() string {
	return fmt.Sprintf("error occurred while writing data to socket: %s", e.ErrStr)
}
func (e SocketWriteError) Code() PQCode {
	return ErrSocketWrite
}

type SocketClosedError struct{}

func (e SocketClosedError) Error() string {
	return "session closed"
}
func (e SocketClosedError) Code() PQCode {
	return ErrSocketClosed
}
func (e SocketClosedError) IsSessionCloseable() {}

type UnhandledError struct {
	ErrStr string
}

func (e UnhandledError) Error() string {
	return "unhandled error : " + e.ErrStr
}

func (e UnhandledError) Code() PQCode {
	return ErrInternal
}

func (e UnhandledError) IsBrokerStoppable() {}

// message or field error
type MarshalAnyFailedError struct{}

func (e MarshalAnyFailedError) Error() string {
	return "marshaling proto message to any message failed"
}

func (e MarshalAnyFailedError) Code() PQCode {
	return ErrMarshalAnyFailed
}

func (e MarshalAnyFailedError) IsSessionCloseable() {}

type UnmarshalAnyFailedError struct{}

func (e UnmarshalAnyFailedError) Error() string {
	return "unmarshaling any message to proto message failed"
}

func (e UnmarshalAnyFailedError) Code() PQCode {
	return ErrUnmarshalAnyFailed
}

func (e UnmarshalAnyFailedError) IsSessionCloseable() {}

type MarshalFailedError struct{}

func (e MarshalFailedError) Error() string {
	return "marshaling any message to bytes failed"
}

func (e MarshalFailedError) Code() PQCode {
	return ErrMarshalFailed
}

func (e MarshalFailedError) IsSessionCloseable() {}

type UnmarshalFailedError struct{}

func (e UnmarshalFailedError) Error() string {
	return "unmarshaling bytes to any message failed"
}

func (e UnmarshalFailedError) Code() PQCode {
	return ErrUnmarshalFailed
}

func (e UnmarshalFailedError) IsSessionCloseable() {}

type InvalidMsgTypeToUnpackError struct {
	Type string
}

func (e InvalidMsgTypeToUnpackError) Error() string {
	return "invalid message type to unpack on " + e.Type
}

func (e InvalidMsgTypeToUnpackError) Code() PQCode {
	return ErrInvalidMsgTypeToUnpack
}

func (e InvalidMsgTypeToUnpackError) IsSessionCloseable() {}

type TopicNotSetError struct{}

func (e TopicNotSetError) Error() string {
	return "topic isn't set"
}

func (e TopicNotSetError) Code() PQCode {
	return ErrTopicNotSet
}

func (e TopicNotSetError) IsSessionCloseable() {}

// DBError
type QRocksOperateError struct {
	ErrStr string
}

func (e QRocksOperateError) Error() string {
	return "rocksdb operate error : " + e.ErrStr
}

func (e QRocksOperateError) Code() PQCode {
	return ErrDBOperate
}

type AlreadyConnectedError struct {
	Addr string
}

func (e AlreadyConnectedError) Error() string {
	return "already connected to " + e.Addr
}

func (e AlreadyConnectedError) Code() PQCode {
	return ErrAlreadyConnected
}

type DialFailedError struct {
	Addr string
	Err  error
}

func (e DialFailedError) Error() string {
	return fmt.Sprintf("dial to %s failed : %v", e.Addr, e.Err)
}

func (e DialFailedError) Code() PQCode {
	return ErrDialFailed
}

type NotConnectedError struct{}

func (e NotConnectedError) Error() string {
	return "there's no connection to any endpoint"
}

func (e NotConnectedError) Code() PQCode {
	return ErrNotConnected
}

type TopicBrokersNotExistError struct{}

func (e TopicBrokersNotExistError) Error() string {
	return "topic broker not exists"
}

func (e TopicBrokersNotExistError) Code() PQCode {
	return ErrTopicBrokersNotExist
}
