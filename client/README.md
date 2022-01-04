# ShapleQ Client
If you want ShapleQ Client only, just type `go get github.com/paust-team/ShapleQ/client`

## Configurations
Similar to setting up a broker, we support the configuration below to set up the client.

```yaml
bootstrap: # bootstrap servers (zookeeper or broker)
  servers: localhost:2181 
  timeout: 3000
timeout: 3 # connection timeout (seconds)
log-level: INFO # DEBUG/INFO/WARNING/ERROR
```

## Usage
Before running client CLI, ShapleQ broker and zookeeper must be running

### Common Flags
- `--broker-timeout` connection timeout
- `--log-level` log level

### Commands
#### Create topic
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/admin/config.yml)
	- `-n, --topic` topic name
	- `-m, --topic-meta` topic meta or description
	- `--broker-address` broker address to connect (ex. 172.32.0.1:1101)

```shell
$ shapleq-cli topic create -n [topic-name] -m [topic-description]
```

#### Delete topic
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/admin/config.yml)
	- `-n, --topic` topic name `required`
	- `--broker-address` broker address (ex. 172.32.0.1:1101)

```shell
$ shapleq-cli topic delete -n [topic-name]
```

#### List topic
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/admin/config.yml)
	- `--broker-address` broker address (ex. 172.32.0.1:1101)
	
```shell
$ shapleq-cli topic list
```

#### Describe topic
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/admin/config.yml)
	- `-n, --topic` topic name `required`
	- `--broker-address` broker address (ex. 172.32.0.1:1101)

```shell
$ shapleq-cli topic describe -z [zk-host] -n [topic-name]
```

#### Publish topic data
***NOTE: The topic must be created before publishing the data related to it(see Create topic command)***
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/producer/config.yml)
	- `-n, --topic` topic name `required`
	- `-f, --file-path` file path to publish (read from file and publish data line by)
	- `--bootstrap-servers` bootstrap server addresses (ex. localhost:2181,localhost:2182)
	- `--bootstrap-timeout` timeout for bootstrapping

```shell
$ shapleq-cli publish [byte-string-data-to-publish] -n [topic-name] --bootstrap-servers 172.32.0.1:2181
```

#### Subscribe topic data
***NOTE: At least one data must be published before subscribe the topic***
- **Flags**
	- `-i, --config-path` config path (default: ~/.shapleq/config/consumer/config.yml)
	- `-n, --topic` topic name `required`
	- `-o, --offset` start offset (default 0)
	- `--bootstrap-servers` bootstrap server addresses (ex. localhost:2181,localhost:2182)
	- `--bootstrap-timeout` timeout for bootstrapping
	
```shell
$ shapleq-cli subscribe -n [topic-name] --bootstrap-servers 172.32.0.1:2181
```

Subscribe command will not stop until broker receives `sigint` or `sigterm`

## Development Guide
You can make your application using the Producer, Consumer and Admin client library.

### Producer
The `producer` is a client that sends the produced data to the broker. Any developer who wants to publish data to ShapleQ Network can make the ShapleQ client application using the `producer` library.

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

// Initialize new producer with config
producerConfig := config.NewProducerConfig()
producerConfig.Load(configPath) // optional
producer := client.NewProducer(producerConfig, topicName)
if err := producer.Connect(); err != nil {
	log.Fatal(err)
}

// Publish records
// This is an example of publishing the data synchronously
// We support asynchornous write using goroutine too. Check out `AsyncPublish`.

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
			
fmt.Println("publish finished")
```

### Consumer
The `consumer` is a client that subscribes to produced data from the broker. Any developer who wants to subscribe to the data related to a specific topic can make the ShapleQ client application using the `consumer` library.

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
			fmt.Println(err)			return
		}
	}
}()

```

