# ShapleQ
Reference of ShapleQ Broker and Client for golang.

## Introduction
ShapleQ has a great mission to offer delivery of data streams over distribute network in real-time.

### Brokers
Brokers connect data streams between producers and consumers. It also operates the flow of data transformation on request of consumer applications. Broker network is composed of multiple brokers and it behaves like a single broker. Clients don't have to specify a particular broker. Additionally, brokers store data temporarily according to its policy, so that clients can utilize the broker network as a storage system.

### Producers
Producers transfer data streams related to the topic to brokers. Producers can set policies of replication and distribution of data.

### Consumers
Consumers receive data streams from brokers. Since brokers send data to consumers actively and manage its frequency according to consumer configurations, so consumers don't have to poll continuously to receive data, unlike other data stream platform. 

## Installation
### Use Docker
1. Install [docker](https://docs.docker.com/get-docker/) and [docker compose](https://docs.docker.com/compose/install/) 
2. Download the source and build docker image as name shapleq (gonna be changed to get image from docker hub)
```
$ git clone https://github.com/paust-team/ShapleQ && cd ShapleQ
$ docker build -t shapleq .
```
3. Run docker compose. Docker-compose examples are in the `examples/docker` directory.
```
$ docker-compose -f {file name} up
```

### Build & Install From Source
1. Install the following prerequisites.
* [git](https://git-scm.com)
* [golang](https://golang.org/dl/) v1.16 or later
* [zookeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html#sc_Download)
* [dep](https://golang.github.io/dep/)
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

2. Download the ShapleQ source.
```
$ git clone https://github.com/paust-team/ShapleQ
```
3. Run `make build`
```
$ cd ShapleQ
$ make build
```
4. Run `make install` and execute ShapleQ CLI 
```
$ make install 
$ shapleq 
ShapleQ cli

Usage:
  shapleQ [command]

Available Commands:
  help        Help about any command
  start       start shapleQ broker
  stop        stop shapleQ broker
  status      show status of shapleQ broker

Flags:
  -h, --help   help for shapleQ

Use "shapleq [command] --help" for more information about a command.
```
## Configurations
We support below configurations to setup broker.

```yaml
# config.yml
hostname: 127.0.0.1 # broker hostname
port: 1101  # broker port
log-dir: ~/.shapleq/log # log directory
data-dir: ~/.shapleq/data # data directory
log-level: DEBUG # DEBUG/INFO/WARNING/ERROR
timeout: 3000 # socket timeout
zookeeper:
  quorum: localhost:2181
  timeout: 3000 # zookeeper connection timeout(milliseconds)
```

Default template for configuring the broker is located at `broker/config/config.yml`. And every config files will be installed in `${HOME}/.shapleq/config` via `make install-config` command. 

## Usage
### Broker
Before running broker, zookeeper server must be running.

```shell
$ zkServer start
```

#### Start broker
- **Flags** (***Flags will override the configurations in the config file***)
	- `-i, --config-path` config path (default: ~/.shapleq/config/broker/config.yml)
	- `-d, --daemon` run with background
	- `-c, --clear` clear data directory after broker stopped (for testing)
	- `--port` port
	- `--zk-quorum` zookeeper quorum
	- `--zk-timeout` zookeeper timeout
	- `-—log-level` log level : 0-debug, 1-info, 2-warning, 3-error
	- `—-log-dir` directory for saving log file
	- `—-data-dir` directory for saving data file

```shell
$ shapleq start --port 11010 -d
```

#### Stop broker

```shell
$ shapleq stop
```

#### Status of broker

```shell
$ shapleq status
```

### Client
- **[Client documentation](https://github.com/paust-team/shapleq/tree/master/client#shapleq-client)**

## License
- GPLv3
