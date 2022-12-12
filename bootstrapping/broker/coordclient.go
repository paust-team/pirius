package broker

import (
	"context"
	"github.com/paust-team/shapleq/bootstrapping/path"
	"github.com/paust-team/shapleq/coordinating"
	"github.com/paust-team/shapleq/logger"
	"go.uber.org/zap"
)

type CoordClientBrokerWrapper struct {
	coordClient coordinating.CoordClient
}

func NewCoordClientWrapper(coordClient coordinating.CoordClient) CoordClientBrokerWrapper {
	return CoordClientBrokerWrapper{coordClient: coordClient}
}

func (t CoordClientBrokerWrapper) AddBroker(host string) error {
	if _, err := t.coordClient.Get(path.BrokersPath).Run(); err != nil {
		logger.Info("creating brokers path")
		if err = t.coordClient.Create(path.BrokersPath, []byte{}).Run(); err != nil {
			return err
		}
	}

	return t.coordClient.
		Create(path.BrokerSequentialNamePrefix(), []byte(host)).
		AsEphemeral().
		AsSequential().
		Run()
}

func (t CoordClientBrokerWrapper) GetBrokers() ([]string, error) {
	if brokers, err := t.coordClient.Children(path.BrokersPath).Run(); err != nil {
		return nil, err
	} else if len(brokers) > 0 {
		return brokers, nil
	} else {
		return nil, nil
	}
}

func (t CoordClientBrokerWrapper) GetBroker(name string) (string, error) {
	data, err := t.coordClient.Get(path.BrokerPath(name)).Run()
	if err != nil {
		return "", err
	}
	return string(data[:]), nil
}

// WatchBrokersPathChanged : register a watcher on children changed and retrieve updated brokers
func (t CoordClientBrokerWrapper) WatchBrokersPathChanged(ctx context.Context) (chan []string, error) {
	ch, err := t.coordClient.Children(path.BrokersPath).Watch(ctx)
	if err != nil {
		return nil, err
	}

	brokersCh := make(chan []string)
	go func() {
		defer close(brokersCh)
		for event := range ch {
			if event.Type == coordinating.EventNodeChildrenChanged {
				brokers, err := t.GetBrokers()
				if err != nil {
					logger.Error("error occurred on receiving watch event", zap.Error(err))
				}
				brokersCh <- brokers
			}
		}
	}()
	return brokersCh, nil
}
