package qerror

import (
	"fmt"
)

type PQError interface {
	Code() QErrCode
	Error() string
}

// bootstrapping
type TopicNotExistError struct {
	Topic string
}

func (e TopicNotExistError) Error() string {
	return fmt.Sprintf("topic(%s) does not exist", e.Topic)
}

func (e TopicNotExistError) Code() QErrCode {
	return ErrTopicNotExists
}

type TargetNotExistError struct {
	Target string
}

func (e TargetNotExistError) Error() string {
	return fmt.Sprintf("target(%s) does not exist", e.Target)
}

func (e TargetNotExistError) Code() QErrCode {
	return ErrTargetNotExists
}

// coordinating
type CoordConnectionError struct {
	Addrs []string
}

func (e CoordConnectionError) Error() string {
	return fmt.Sprintf("failed to connect to coordinator %s\n", e.Addrs)
}

func (e CoordConnectionError) Code() QErrCode {
	return ErrCoordConnection
}

type CoordRequestError struct {
	ErrStr string
}

func (e CoordRequestError) Error() string {
	return "pqerror occurred during request to zookeeper : " + e.ErrStr
}

func (e CoordRequestError) Code() QErrCode {
	return ErrCoordRequest
}

type CoordTargetAlreadyExistsError struct {
	Target string
}

func (e CoordTargetAlreadyExistsError) Error() string {
	return fmt.Sprintf("target %s already exists=\n", e.Target)
}

func (e CoordTargetAlreadyExistsError) Code() QErrCode {
	return ErrCoordTargetAlreadyExists
}

type CoordLockFailError struct {
	LockPath string
	ErrStr   string
}

func (e CoordLockFailError) Error() string {
	return fmt.Sprintf("locking path(%s) failed : %s\n", e.LockPath, e.ErrStr)
}

func (e CoordLockFailError) Code() QErrCode {
	return ErrCoordLockFail
}

type CoordEncodeFailError struct{}

func (e CoordEncodeFailError) Error() string {
	return "failed to encode target to bytes"
}

func (e CoordEncodeFailError) Code() QErrCode {
	return ErrCoordEncodeFail
}

type CoordDecodeFailError struct{}

func (e CoordDecodeFailError) Error() string {
	return "failed to decode bytes to target"
}

func (e CoordDecodeFailError) Code() QErrCode {
	return ErrCoordDecodeFail
}

type CoordNothingToRemoveError struct{}

func (e CoordNothingToRemoveError) Error() string {
	return "no target to remove from zookeeper"
}

func (e CoordNothingToRemoveError) Code() QErrCode {
	return ErrCoordNothingToRemove
}

type CoordOperateError struct {
	ErrStr string
}

func (e CoordOperateError) Error() string {
	return "Coord operate error : " + e.ErrStr
}

func (e CoordOperateError) Code() QErrCode {
	return ErrCoordOperate
}

type CoordNoNodeError struct {
	Path string
}

func (e CoordNoNodeError) Error() string {
	return "no node exists for path: " + e.Path
}

func (e CoordNoNodeError) Code() QErrCode {
	return ErrCoordNoNode
}
