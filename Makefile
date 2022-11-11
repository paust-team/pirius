GOPATH	?= $(shell go env GOPATH)
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
THIRDPARTY_DIR := $(abspath _thirdparty)
GIT_DIR := .git

mac-os-host := $(findstring Darwin, $(shell uname))
linux-os-host := $(findstring Linux, $(shell uname))

# bin
BROKER_BIN_DIR := broker/cmd/shapleq
CLIENT_BIN_DIR := client/cmd/shapleq-client
BROKER_BIN_NAME := shapleq
CLIENT_BIN_NAME := shapleq-client
BROKER_BIN := $(BROKER_BIN_DIR)/$(BROKER_BIN_NAME)
CLIENT_BIN := $(CLIENT_BIN_DIR)/$(CLIENT_BIN_NAME)

INSTALL_BIN_DIR := /usr/local/bin

# config
CONFIG_NAME := config
BROKER_CONFIG_DIR := broker/config
ADMIN_CONFIG_DIR := client/config/admin
PRODUCER_CONFIG_DIR := client/config/producer
CONSUMER_CONFIG_DIR := client/config/consumer

INSTALL_CONFIG_HOME_DIR := ${HOME}/.shapleq/config
INSTALL_BROKER_CONFIG_DIR := ${INSTALL_CONFIG_HOME_DIR}/broker
INSTALL_ADMIN_CONFIG_DIR := ${INSTALL_CONFIG_HOME_DIR}/admin
INSTALL_PRODUCER_CONFIG_DIR := ${INSTALL_CONFIG_HOME_DIR}/producer
INSTALL_CONSUMER_CONFIG_DIR := ${INSTALL_CONFIG_HOME_DIR}/consumer

# rocksdb
ROCKSDB_DIR := $(THIRDPARTY_DIR)/rocksdb
ROCKSDB_BUILD_DIR := $(ROCKSDB_DIR)/build
ROCKSDB_INSTALL_DIR := $(ROCKSDB_DIR)/install
ROCKSDB_INCLUDE_DIR := $(ROCKSDB_INSTALL_DIR)/include
ROCKSDB_LIB_DIR := $(ROCKSDB_INSTALL_DIR)/lib
LIB_ROCKSDB := $(ROCKSDB_LIB_DIR)/librocksdb.a

# proto
PROTOC_DIR := $(THIRDPARTY_DIR)/protoc
PROTOC := $(PROTOC_DIR)/bin/protoc
PROTOC_GEN_GO := $(GOPATH)/bin/protoc-gen-go

DEPLOY_TARGET ?= debug # debug | release

$(PROTOC):
	mkdir -p $(PROTOC_DIR)
ifdef mac-os-host
	wget -O $(PROTOC_DIR)/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v3.19.4/protoc-3.19.4-osx-x86_64.zip
endif
ifdef linux-os-host
	wget -O $(PROTOC_DIR)/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v3.19.4/protoc-3.19.4-linux-x86_64.zip
endif
	cd $(PROTOC_DIR) && unzip protoc.zip
	chmod +x $(PROTOC)

$(PROTOC_GEN_GO):
	go install github.com/golang/protobuf/protoc-gen-go

$(PROTOC_GEN_GRPC):
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

PROTO_DIR := ./proto
PROTO_TARGETS := $(PROTO_DIR)/agent.pb.go $(PROTO_DIR)/broker.pb.go
.SUFFIXES: .proto .pb.go
%.pb.go: %.proto $(PROTOC_GEN_GO) $(PROTOC_GEN_GRPC) $(PROTOC)
	$(PROTOC) --proto_path=$(PROTO_DIR) --go_out=$(PROTO_DIR) --go-grpc_out=$(PROTO_DIR) $<

.PHONY: compile-protobuf prepare
compile-protobuf: $(PROTO_TARGETS) $(PROTOC_GEN_GO) $(PROTOC)

$(LIB_ROCKSDB):
	if [ -d "$(GIT_DIR)" ]; then \
		git submodule update --init --recursive; \
	fi
	mkdir -p $(ROCKSDB_BUILD_DIR)
ifdef mac-os-host
	cd $(ROCKSDB_BUILD_DIR) &&\
 	cmake .. -DCMAKE_TARGET_MESSAGES=OFF -DCMAKE_BUILD_TYPE=RelWithDebInfo -DPORTABLE=ON -DWITH_TESTS=OFF \
		-DWITH_BENCHMARK_TOOLS=OFF -DWITH_SNAPPY=ON -DUSE_RTTI=ON -DWITH_GFLAGS=OFF -DCMAKE_INSTALL_PREFIX=$(ROCKSDB_INSTALL_DIR) -DROCKSDB_BUILD_SHARED=OFF &&\
	make -j`nproc`
endif
ifdef linux-os-host
	cd $(ROCKSDB_BUILD_DIR) &&\
	cmake .. -DCMAKE_TARGET_MESSAGES=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo \
		-DPORTABLE=ON -DWITH_FALLOCATE=OFF -DWITH_TESTS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DWITH_SNAPPY=ON -DUSE_RTTI=ON -DWITH_GFLAGS=OFF -DCMAKE_INSTALL_PREFIX=$(ROCKSDB_INSTALL_DIR) -DROCKSDB_BUILD_SHARED=OFF &&\
	make -j`nproc`
endif
	cd $(ROCKSDB_BUILD_DIR) && make install
	touch $(LIB_ROCKSDB)

.PHONY: build-rocksdb
build-rocksdb: $(LIB_ROCKSDB)

$(BROKER_BIN): $(LIB_ROCKSDB)
ifdef linux-os-host
	CGO_ENABLED=1 CGO_CFLAGS="${CGO_CFLAGS} -I$(ROCKSDB_INCLUDE_DIR)" \
	CGO_LDFLAGS="${CGO_LDFLAGS} -L$(ROCKSDB_LIB_DIR)" \
	GOOS=linux GOARCH=amd64 \
	go build -tags $(DEPLOY_TARGET) -o $(BROKER_BIN) ./$(BROKER_BIN_DIR)/main.go
endif
ifdef mac-os-host
	CGO_ENABLED=1 CGO_CFLAGS="${CGO_CFLAGS} -I$(ROCKSDB_INCLUDE_DIR)" \
	CGO_LDFLAGS="${CGO_LDFLAGS} -L$(ROCKSDB_LIB_DIR)" \
	go build -tags $(DEPLOY_TARGET) -o $(BROKER_BIN) ./$(BROKER_BIN_DIR)/main.go
endif
	touch $(BROKER_BIN)

$(CLIENT_BIN):
	go build -tags $(DEPLOY_TARGET) -o $(CLIENT_BIN) ./$(CLIENT_BIN_DIR)/main.go

.PHONY: build-broker build-client
build-broker: $(BROKER_BIN)
build-client: $(CLIENT_BIN)

.PHONY: all build rebuild install test clean-rocksdb clean-proto
build: build-broker build-client install-config
all: build

install-config:
	mkdir -p ${INSTALL_BROKER_CONFIG_DIR} && cp ${BROKER_CONFIG_DIR}/${CONFIG_NAME}.yml ${INSTALL_BROKER_CONFIG_DIR}/
	mkdir -p ${INSTALL_ADMIN_CONFIG_DIR} && cp ${ADMIN_CONFIG_DIR}/${CONFIG_NAME}.yml ${INSTALL_ADMIN_CONFIG_DIR}/
	mkdir -p ${INSTALL_PRODUCER_CONFIG_DIR} && cp ${PRODUCER_CONFIG_DIR}/${CONFIG_NAME}.yml ${INSTALL_PRODUCER_CONFIG_DIR}/
	mkdir -p ${INSTALL_CONSUMER_CONFIG_DIR} && cp ${CONSUMER_CONFIG_DIR}/${CONFIG_NAME}.yml ${INSTALL_CONSUMER_CONFIG_DIR}/


install: build
	cp ${BROKER_BIN_DIR}/${BROKER_BIN_NAME} ${INSTALL_BIN_DIR}/
	cp ${CLIENT_BIN_DIR}/${CLIENT_BIN_NAME} ${INSTALL_BIN_DIR}/

clean:
	rm -f $(BROKER_BIN)
	rm -f $(CLIENT_BIN)
	rm -rf $(INSTALL_CONFIG_HOME_DIR)

clean-rocksdb:
	rm -rf $(ROCKSDB_BUILD_DIR)
	rm -rf $(ROCKSDB_INSTALL_DIR)

clean-proto:
	rm -rf $(PROTO_TARGETS)

test:
	@go test -v
