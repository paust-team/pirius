# ShapleQ
Reference of ShapleQ Broker and Client for golang.

## Introduction
PaustQ has a great mission to offer publishing and subscribing **Data Streams** with **Custom Operations** over **P2P Network**. 

## Features
- Continuously produce stream data and consume it
- Support Multi Broker
- Support Multi Topic
- Support Multi Client (Consumer, Producer)

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
	- `—home-dir` home directory for broker to store information data (default ~/.shapleQ)
	- `—log-dir` directory for saving log file (default ~/.shapleQ/log)
	- `—data-dir` directory for saving data file (default ~/.shapleQ/data)


```shell
$ shapleQ start -z [zk-host]
```

#### Stop broker
- **Flags**
	- `--dir` home path for broker (default ~/.paustq)


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
