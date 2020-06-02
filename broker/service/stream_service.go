package service

import (
	"context"
	"github.com/paust-team/paustq/broker/internals"
)

type StreamService struct {
}

func HandleNewSession(brokerCtx context.Context, sessionCh chan *internals.Session) {
	// Pipeline

}
