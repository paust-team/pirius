package bootstrapping

import (
	"github.com/paust-team/pirius/bootstrapping/broker"
	"github.com/paust-team/pirius/bootstrapping/topic"
	"github.com/paust-team/pirius/coordinating"
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
