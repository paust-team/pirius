package qerror

import (
	"fmt"
	"sync"
)

type PQError interface {
	Code() QErrCode
	Error() string
}

type IsSessionCloseable interface {
	IsSessionCloseable()
}

func MergeErrors(errChannels ...<-chan error) <-chan error {
	var wg sync.WaitGroup

	out := make(chan error, len(errChannels))
	output := func(c <-chan error) {
		defer wg.Done()
		for n := range c {
			out <- n
		}
	}

	wg.Add(len(errChannels))
	for _, c := range errChannels {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
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

func (e TopicNotExistError) IsSessionCloseable() {}

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

func (e CoordRequestError) IsSessionCloseable() {}

type CoordTargetAlreadyExistsError struct {
	Target string
}

func (e CoordTargetAlreadyExistsError) Error() string {
	return fmt.Sprintf("target %s already exists=\n", e.Target)
}

func (e CoordTargetAlreadyExistsError) Code() QErrCode {
	return ErrCoordTargetAlreadyExists
}

//func (e CoordTargetAlreadyExistsError) IsSessionCloseable() {}

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

func (e CoordLockFailError) IsSessionCloseable() {}

type CoordEncodeFailError struct{}

func (e CoordEncodeFailError) Error() string {
	return "failed to encode target to bytes"
}

func (e CoordEncodeFailError) Code() QErrCode {
	return ErrCoordEncodeFail
}

func (e CoordEncodeFailError) IsSessionCloseable() {}

type CoordDecodeFailError struct{}

func (e CoordDecodeFailError) Error() string {
	return "failed to decode bytes to target"
}

func (e CoordDecodeFailError) Code() QErrCode {
	return ErrCoordDecodeFail
}

func (e CoordDecodeFailError) IsSessionCloseable() {}

type CoordNothingToRemoveError struct{}

func (e CoordNothingToRemoveError) Error() string {
	return "no target to remove from zookeeper"
}

func (e CoordNothingToRemoveError) Code() QErrCode {
	return ErrCoordNothingToRemove
}

func (e CoordNothingToRemoveError) IsSessionCloseable() {}

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
