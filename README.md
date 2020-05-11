# ShapleQ
Reference of ShapleQ Broker and Client for golang.

## Introduction
PaustQ has a great mission to offer publishing and subscribing **Data Streams** with **Custom Operations** over **P2P Network**. 

## Installation

### Prerequisite
* [git](https://git-scm.com)
* [make](https://www.gnu.org/software/make/)
* [golang](https://golang.org/dl/) v1.17 or later
* [zookeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html#sc_Download)
### Install from source code
```
go get github.com/paust-team/ShapleQ
cd $GOPATH/github.com/paust-team/ShapleQ
make build
make install 
```

## Usage
### Broker
Before running broker, zookeeper server must be running
```
zkServer start
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
shapleQ start -z 127.0.0.1
```

#### Stop broker
- **Flags**
	- `--dir` home path for broker (default ~/.paustq)
```shell
shapleQ stop
```
#### Status of broker
```shell
shapleQ status
```

### Client
- **[Client documentation](https://github.com/paust-team/paustq/client)**

