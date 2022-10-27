package bootstrapping

import (
	"github.com/paust-team/shapleq/bootstrapping/broker"
	"github.com/paust-team/shapleq/bootstrapping/topic"
	"github.com/paust-team/shapleq/coordinating"
)

type BootstrapService struct {
	topic.CoordClientTopicWrapper
	broker.CoordClientBrokerWrapper
}

func NewBootStrapService(coordClient coordinating.CoordClient) *BootstrapService {
	return &BootstrapService{
		CoordClientTopicWrapper:  topic.NewCoordClientTopicWrapper(coordClient),
		CoordClientBrokerWrapper: broker.NewCoordClientWrapper(coordClient),
	}
}
