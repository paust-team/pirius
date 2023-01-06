# Pirius
Pirius has a great mission to deliver data streams over a distributed network in real-time.

Unlike kafka, Pirius aims to efficiently deliver messages on distrubuted and decentralized networks through P2P connections between `agent` while minimizing the role of `broker`.

### Broker
The pirius broker helps to connect data streams between publishers and subscribers. When a publisher or subscriber for a specific topic appears, it manages topic fragments to distribute the topic to multiple publishers and coordinates subscription for subscriber to find publishers to connect.

### Agent
The pirius agent is a messeage queue library that can be used in any application that requires a pubsub mechanism over a distributed(or decentralized) network.
An agent can be used for two types of clients: `Publisher` and `Subscriber`.

A `Publisher` transfers data streams related to specific topic to `Subscribers`. And a `Subscriber` receives data streams from `Publishers`. Since the publisher sends data to the subscriber actively and manages throughput of stream(batch), so unlike other data stream platform, the subscriber don't have to ask for next data to receive directly.


## Quick Start
### Broker
#### Using [docker](https://docs.docker.com/get-docker/)
1. Download the source and build docker image
```
$ git clone https://github.com/paust-team/pirius && cd pirius
$ docker build -t pirius .
```
2. Start pirius image
```
$ docker run -p 1101:1101 --rm -d pirius
```


#### Build & Install From Source
1. Install the following prerequisites.
* [git](https://git-scm.com)
* [golang](https://golang.org/dl/) v1.18+
* [zookeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html#sc_Download) v3.5.0+
* [go-dep](https://golang.github.io/dep/)
  ```
  $ curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
  ```
* libtool, autoconf, coreutils, cmake(higher than v3.5.1)

  **Debian**
  ```
  $ apt-get update && apt-get install g++ libtool autoconf coreutils cmake libsnappy-dev libzstd-dev liblz4-dev zlib1g-dev
  ```
  **MacOS**
    ```
  $ brew install libtool autoconf coreutils cmake automake snappy zstd
  $ xcode-select --install // if command line tools are not installed,
  ```

2. Download the Pirius source.
```
$ git clone https://github.com/paust-team/pirius
```
3. Run `make build`
```
$ cd Pirius
$ make clean
$ make build-broker
$ make install-config
```
4. Execute pirius CLI. Before running broker, zookeeper server must be running state.
```
$ cd broker/cmd
$ ./pirius 
Pirius cli

Usage:
  pirius [command]

Available Commands:
  help        Help about any command
  start       start pirius broker
  stop        stop pirius broker

Flags:
  -h, --help   help for pirius

Use "pirius [command] --help" for more information about a command.
```

#### Configuration
We support below configurations to setup broker.
```yaml
# pirius/broker/config/config.yml
bind: 127.0.0.1 # use 0.0.0.0 to connect from remote host
host: 127.0.0.1 # broker host address. private or public ip of host
port: 1101  # broker port  
log-level: DEBUG # DEBUG/INFO/WARNING/ERROR  
timeout: 10000  
zookeeper:  
  quorum: localhost:2181 # zk-quorum (addr1:port1,addr2:port2...)
  timeout: 5000 # zk connection timeout
```

Default template for configuring the broker is located at `pirius/config/config.yml`. And every config files will be installed in `${HOME}/.pirius/config` via `make install-config` command by default.

### Agent
Currently, standalone agent is not supported officially. But there is sample standalone agent as example  in `agent/cmd` path.

To build sample agent and run it, follow below guide. The pirius broker should be running before starting agents.
```
$ cd pirius
$ make build-agent
$ cd agent/cmd

# create topic
$ ./pirius-agent topic create -t test-topic

# run publisher
# it will send sequential numbered data to all subscribers of a topic per a second.
$ ./pirius-agent start-publish -t test-topic

# run subscriber
# it will receive sequential numbered data from all publishers of a topic.
$ ./pirius-agent start-subscribe -t test-topic

# when running multiple publishers or subscribers, data-dir should be different to avoid collision of Rocksdb's data path.
$ ./pirius-agent start-publish -t test-topic --data-dir ./pub1 &
$ ./pirius-agent start-publish -t test-topic --data-dir ./pub2
```

If you are looking for other examples, check out agent tests(`agent/agent_test.go`) or integration tests(test/integration_test.go).

#### Configuration
```yaml
# pirius/agent/config/config.yml
bind: 127.0.0.1 # bind address  
host: 192.168.0.10 # agent host address  
port: 11010  # agent port  
log-dir: ~/.pirius/log # log directory  
data-dir: ~/.pirius/data # directory for storing agent-meta and data
db-name: pirius-store  
log-level: DEBUG # DEBUG/INFO/WARNING/ERROR  
timeout: 10000  
retention: 1 # data retention period for publisher (day)
retention-check-interval: 10000 # millisecond  
zookeeper:  
  quorum: localhost:2181 # zk-quorum (addr1:port1,addr2:port2...)
  timeout: 5000  # zk connection timeout
  flush-interval: 2000
```


#### PubSubAgent
The `PubSubAgent` is basic pirius agent. `StartPublish` is a publisher method to start data stream to publish for a topic and `StartSubscribe` is a subscriber method to start data stream to subscribe for a topic. The `PubSubAgent` can be used aOr, it can be both publisher and subscriber at the same time.

#### RetrievablePubSubAgent
The `RetrievablePubSubAgent` is a agent that can be used for a more specific purpose than `PubSubAgent`. It is designed for a case when the publisher needs to receive the results after the subscriber consumed the data received from it.

