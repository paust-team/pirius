package pqerror

import (
	"context"
	"github.com/paust-team/paustq/broker/internals"
	"sync"
)

type PQError interface {
	Code() PQCode
	Error() string
}

type IsSessionCloseable interface {
	IsSessionCloseable()
}

// If error is client visible, error message gonna be sent to related client
type IsClientVisible interface {
	IsClientVisible()
}

// If error is broadcastable, all of the clients get error message
type IsBrokerStoppable interface {
	IsBrokerStoppable()
}

type IsBroadcastable interface {
	IsBroadcastable()
}

type SessionError struct {
	Err           PQError
	Session       *internals.Session
	CancelSession context.CancelFunc
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

func MergeSessionErrors(errChannels ...<-chan SessionError) <-chan SessionError {
	var wg sync.WaitGroup

	out := make(chan SessionError, len(errChannels))
	output := func(c <-chan SessionError) {
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
