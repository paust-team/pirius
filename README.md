# ShapleQ
Reference of ShapleQ Broker and Client for golang.

## Introduction
ShapleQ has a great mission to offer delivery of data streams over distribute network. ShapleQ is specialized at delivering data on circumstance that number of consumer is greater than number of producers. 

### Broker
Broker connects data streams between producers and consumers. It also operates flow of data transformation on request of consumer applications. Broker network is composed of multiple brokers and it behaves like a single broker. Clients don't have to specify a particular broker. Broker stores data temporarily according to its policy, so that clients can utilize broker as a storage system.

### Producer
Producer transfers data streams related with topic to brokers. Producer can set policy of replication and distribution of data.

### Consumer
Consumer receives data streams from brokers. Since broker sends data to consumers actively and manage its frequency according to consumer configurations, so consumer doesn't have to poll continuously to receive data unlike other data stream platform. 

## Installation

### From Source
#### Prerequisite
* [git](https://git-scm.com)
* [make](https://www.gnu.org/software/make/)
* [golang](https://golang.org/dl/) v1.13 or later
* [zookeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html#sc_Download)

#### Install
```
$ go get github.com/paust-team/ShapleQ
$ cd $GOPATH/github.com/paust-team/ShapleQ
$ make build
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
  -h, --help   help for paustq

Use "shapleQ [command] --help" for more information about a command.
```


## Usage
### Broker
Before running broker, zookeeper server must be running

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
- **[Client documentation](https://github.com/paust-team/paustq/tree/master/client#shapleq-client)**

## License
- GPLv3
