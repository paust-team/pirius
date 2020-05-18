# ShapleQ Client
If you want ShapleQ Client only, just type `go get github.com/paust-team/ShapleQ/client`
## Usage
Before running client cli, ShapleQ broker and zookeeper must be running

### Create topic
- **Flags**
	- `-z` zk-address `required`
	- `-n` topic name `required`
	- `-m` topic meta or description


```shell
$ shapleQ-cli topic create -z [zk-host] -n [topic-name] -m [topic-description]
```

### Delete topic
- **Flags**
	- `-z` zk-address `required`
	- `-n` topic name `required`


```shell
$ shapleQ-cli topic delete -z [zk-host] -n [topic-name]
```

### List topic
- **Flags**
	- `-z` zk-address `required`


```shell
$ shapleQ-cli topic list -z [zk-host]
```

### Describe topic
- **Flags**
	- `-z` zk-address `required`
	- `-n` topic name `required`


```shell
$ shapleQ-cli topic describe -z [zk-host] -n [topic-name]
```

### Publish topic data
***NOTE: The topic must be created before publish the data to it(see Create topic data cmd)***
- **Flags**
	- `-p` broker port (default 11010)
	- `-z` zk-address `required`
	- `-n` topic name `required`
	- `-f` file path to publish (read from file and publish data line by)

	
```shell
$ shapleQ-cli publish [byte-string-data-to-publish] -n [topic-name] -z [zk-host]
```

### Subscribe topic data
***NOTE: At least one data must be published before subscribe the topic***
- **Flags**
	- `-p` broker port (default 11010)
	- `-z` zk-address `required`
	- `-n` topic name `required`
	- `-o` start offset (default 0)

	
```shell
$ shapleQ-cli subscribe -n [topic-name] -z [zk-host]
```

Subscribe command will not stop until broker had stopped or received `sigint` or `sigterm`

## Development Guide
You can build your own application using Producer, Consumer client library.

### Producer
The `producer` client is a client that sends the produced data to the broker. Any developer who wants to publish data to ShapleQ Network can write the ShapleQ client application using `producer` client library.

### Structs

```go

type Producer struct {/* private variables */}

// Initialize Producer struct using builder: NewProducer(zkHost string)

```

#### Callable Methods
- `Publish(ctx context.Context, data []byte)`
- `WaitAllPublishResponse()`
- `Connect(ctx context.Context, topicName string) error`
- `Close() error`
- `WithLogLevel(level logger.LogLevel) *Producer`
- `WithBrokerPort(port uint16) *Producer`
- `WithTimeout(timeout time.Duration) *Producer`
- `WithChunkSize(size uint32) *Producer`

#### Sample Code

```go
import "github.com/paust-team/paustq/client/producer"

topic := “test”

// Initialize new Producer client with localhost zookeeper
producerClient := producer.NewProducer(“127.0.0.1”)
ctx := context.Background()
if err := producerClient.Connect(ctx, topic); err != nil {
	fmt.Println(err)
	return
}

// Publish records
publishCh, errCh := producerClient.Publish(ctx)

go func() {
	case err, ok := <-errChP:
		if ok {
			fmt.Println(err)
			os.Exit(1)
		}
		return
	case <-ctx1.Done():
		return
}()

testRecords := [][]byte{“1”, “2”, “3”, “4”, “5”}
for _, record := range testRecords {
	publishCh <- record
}

// wait to all data saved on broker
time.Sleep(3 * time.Second)

// Close Producer client
if err := producerClient.Close(); err != nil {
	fmt.Println(err)
}
			
fmt.Println(“publish finished”)
```

### Consumer
The `consumer` client is a client that subscribes the produced data from the broker. Any developer who wants to subscribe data related to specific topic can write the ShapleQ client application using `consumer` client library.

#### Structs

```go

type Consumer struct {/* private variables */}
// Initialize Consumer struct using builder: NewConsumer(zkHost string)

type SinkData struct {
	Error              error
	Data               []byte
	Offset, LastOffset uint64
}
```

#### Callable Methods
- `Subscribe(ctx context.Context, startOffset uint64) (chan SinkData, error)`
- `Connect(ctx context.Context, topicName string) error`
- `Close() error`
- `WithLogLevel(level logger.LogLevel) *Consumer`
- `WithBrokerPort(port uint16) *Consumer`
- `WithTimeout(timeout time.Duration) *Consumer`


#### Sample Code

```go
import "github.com/paust-team/paustq/client/consumer"

topic := “test”

// Initialize new Consumer client with localhost zookeeper
consumerClient := consumer.NewConsumer(“127.0.0.1”)
ctx := context.Background()
if err := consumerClient.Connect(ctx, topic); err != nil {
	fmt.Println(err)
	return
}
startOffset := 0
subscribeCh, errCh := consumerClient.Subscribe(ctx, startOffset)

go func() {
	select {
	case err, ok := <-errCh:
		if ok {
			fmt.Println(err)
			os.Exit(1)
		}
		return
	case <-ctx.Done():
		return
	}
}()

subscribeUntil:
for {
	select {
	case response:= <- subscribeCh:
		fmt.Println("received data:", response.Data)

		// break on reach end
		if response.StartOffset == response.LastOffset  {
			break subscribeUntil
		}
	case <- time.After(time.Second * 1):
		fmt.Println("subscribe timeout")
		return
	}
}

if err := consumerClient.Close(); err != nil {
	fmt.Println(err)
	return
}
fmt.Println(“subscribe finished”)
```
