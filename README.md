# ShapleQ
Reference of ShapleQ Broker and Client for golang.

## Introduction
ShapleQ has a great mission to offer delivery of data streams over distribute network. ShapleQ is specialized in delivering data on a circumstance that the number of consumers is greater than the number of producers. 

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
$ go get github.com/paust-team/ShapleQ
$ cd $GOPATH/github.com/paust-team/ShapleQ
$ docker build -t shapleq .
```
3. Run docker compose
```
$ docker-compose up
```

### Build & Install From Source
1. Install the following prerequisites.
* [git](https://git-scm.com)
* [golang](https://golang.org/dl/) v1.13 or later
* [zookeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html#sc_Download)
* [dep](https://golang.github.io/dep/)
  ```
  $ curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
  ```
* libtool, autoconf, coreutils, cmake(higher than v3.5.1)

  **Debian**
  ```
  $ apt-get update && apt-get install libtool autoconf coreutils cmake
  ```
  **MacOS**
    ```
  $ brew install libtool autoconf coreutils cmake
  ```
2. Download the ShapleQ source.
```
$ go get github.com/paust-team/ShapleQ
```
3. Run `make build`
```
$ cd $GOPATH/github.com/paust-team/ShapleQ
$ make build
```
4. Run `make install` and execute ShapleQ CLI 
```
$ make install 
$ shapleQ 
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

Use "shapleQ [command] --help" for more information about a command.
```

## Usage
### Broker
Before running broker, zookeeper server must be running.

```shell
$ zkServer start
```

#### Start broker
- **Flags**
	- `-d` run with background
	- `-p` port (default 11010)
	- `-z` zk-address `required`
	- `—log-level` log level : 0-debug, 1-info, 2-warning, 3-error (default: 1)
	- `—log-dir` directory for saving log file (default ~/.shapleQ/log)
	- `—data-dir` directory for saving data file (default ~/.shapleQ/data)


```shell
$ shapleQ start -z [zk-host]
```

#### Stop broker

```shell
$ shapleQ stop
```

#### Status of broker

```shell
$ shapleQ status
```

### Client
- **[Client documentation](https://github.com/paust-team/shapleq/tree/master/client#shapleq-client)**

## License
- GPLv3
