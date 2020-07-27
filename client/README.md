# ShapleQ Client
If you want ShapleQ Client only, just type `go get github.com/paust-team/ShapleQ/client`

## Configurations
Similar to setting up a broker, we supports the configuration below to set up a client.

```yaml
broker:
  port: 1101 # broker port
  host: localhost # broker address
timeout: 3 # connection timeout (seconds)
log-level: INFO # DEBUG/INFO/WARNING/ERROR
```

## Usage
Before running client cli, ShapleQ broker and zookeeper must be running

### Common Flags
- `--broker-host` broker host
- `--broker-port` broker port
- `--timeout` connection timeout
- `--log-level` log level

### Commands
#### Create topic
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/admin/config.yml)
	- `-n, --topic` topic name
	- `-m, --topic-meta` topic meta or description

```shell
$ shapleq-cli topic create -n [topic-name] -m [topic-description]
```

#### Delete topic
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/admin/config.yml)
	- `-n, --topic` topic name `required`

```shell
$ shapleq-cli topic delete -n [topic-name]
```

#### List topic
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/admin/config.yml)

```shell
$ shapleq-cli topic list
```

#### Describe topic
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/admin/config.yml)
	- `-n, --topic` topic name `required`

```shell
$ shapleq-cli topic describe -z [zk-host] -n [topic-name]
```

#### Publish topic data
***NOTE: The topic must be created before publishing the data to it(see Create topic data command)***
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/producer/config.yml)
	- `-n, --topic` topic name `required`
	- `-f, --file-path` file path to publish (read from file and publish data line by)

```shell
$ shapleq-cli publish [byte-string-data-to-publish] -n [topic-name] --broker-host 172.32.0.1
```

#### Subscribe topic data
***NOTE: At least one data must be published before subscribe the topic***
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/consumer/config.yml)
	- `-n, --topic` topic name `required`
	- `-o, --offset` start offset (default 0)
	
```shell
$ shapleq-cli subscribe -n [topic-name] --broker-host 172.32.0.1
```

Subscribe command will not stop until broker had stopped or received `sigint` or `sigterm`

## Development Guide
You can build your application using the Producer, Consumer, and Admin client library.

### Producer
The `producer` client is a client that sends the produced data to the broker. Any developer who wants to publish data to ShapleQ Network can write the ShapleQ client application using the `producer` client library.

#### Structs

```go

type Producer struct {/* private variables */}

// Initialize Producer struct using builder: 
- NewProducer(config *config.ProducerConfig, topic string) *Producer
- NewProducerWithContext(ctx context.Context, config *config.ProducerConfig, topic string)

```

#### Callable Methods
- `Context() context.Context`
- `Publish(data []byte)`
- `AsyncPublish(source <-chan []byte) (<-chan common.Partition, <-chan error, error)`
- `Connect() error`
- `Close() error`

#### Sample Code

```go
import "github.com/paust-team/shapleq/client/producer"

topicName := "test"

// Initialize new Producer client with producer config
producerConfig := config.NewProducerConfig()
producerConfig.Load(configPath) // optional
producer := client.NewProducer(producerConfig, topicName)
if err := producer.Connect(); err != nil {
	log.Fatal(err)
}

// Publish records
// This is an example of publishing the topic data with sync mode.
// We support async mode using goroutine too. Check out `AsyncPublish`.

testRecords := [][]byte{"1", "2", "3", "4", "5"}
for _, record := range testRecords {
	partition, err := producer.Publish(record)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("publish succeed, partition id : %d, offset : %d\n", partition.Id, partition.Offset)
}

// Close Producer client
if err := producerClient.Close(); err != nil {
	fmt.Println(err)
}
			
fmt.Println(“publish finished”)
```

### Consumer
The `consumer` client is a client that subscribes to the produced data from the broker. Any developer who wants to subscribe to data related to a specific topic can write the ShapleQ client application using the `consumer` client library.

#### Structs

```go

type Consumer struct {/* private variables */}
// Initialize Consumer struct using builder: 
- NewConsumer(config *config.ConsumerConfig, topic string) *Consumer
- NewConsumerWithContext(ctx context.Context, config *config.ConsumerConfig, topic string) *Consumer

type FetchedData struct {
	Data               []byte
	Offset, LastOffset uint64
}
```

#### Callable Methods
- `Context() context.Context`
- `Subscribe(startOffset uint64) (<-chan FetchedData, <-chan error, error)`
- `Connect() error`
- `Close() error`

#### Sample Code

```go
import "github.com/paust-team/shapleq/client/consumer"

topicName := "test"

consumerConfig := config.NewConsumerConfig()
consumer := client.NewConsumer(consumerConfig, topicName)
if err := consumer.Connect(); err != nil {
	fmt.Println(err)
	return
}

receiveCh, subErrCh, err := consumer.Subscribe(0)
if err != nil {
	fmt.Println(err)
	return
}

go func() {
	defer consumer.Close()
	for {
		select {
		case received := <-receiveCh:
			fmt.Println(received.Data)
			// add custom break condition
			// ex)
			// if len(actualRecords) == len(expectedRecords) {
			//	return
			// }
		case err := <-subErrCh:
			fmt.Println(err)
			return
		}
	}
}()

```

