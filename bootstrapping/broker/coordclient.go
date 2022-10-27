package broker

import "github.com/paust-team/shapleq/coordinating"

type CoordClientBrokerWrapper struct {
	coordClient coordinating.CoordClient
}

func NewCoordClientWrapper(coordClient coordinating.CoordClient) CoordClientBrokerWrapper {
	return CoordClientBrokerWrapper{coordClient: coordClient}
}
