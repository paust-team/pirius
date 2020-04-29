package internals

// Temporary helper for zookeeper
type ZookeeperHelper interface {
	GetTopicEndpoint(string) string
}

type defaultZkHelper struct {}
func NewZookeeperHelper() *defaultZkHelper {
	return &defaultZkHelper{}
}
func (zk *defaultZkHelper) GetTopicEndpoint(topicName string) string {
	return "localhost"
}