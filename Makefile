GOPATH	:= $(shell go env GOPATH)
GIT_DIR := $(shell git rev-parse --git-dir 2>/dev/null || true)
NPROC := 4
THIRDPARTY_DIR := $(abspath _thirdparty)

mac-os-host := $(findstring Darwin, $(shell uname))
linux-os-host := $(findstring Linux, $(shell uname))

# bin
BROKER_BIN_DIR := broker/cmd/shapleq
CLIENT_BIN_DIR := client/cmd/shapleq-client
BROKER_BIN_NAME := shapleq
CLIENT_BIN_NAME := shapleq-client

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
ROCKSDB_INCLUDE_DIR := $(ROCKSDB_DIR)/include
LIB_ROCKSDB := $(ROCKSDB_BUILD_DIR)/librocksdb.a

BASE_CMAKE_FLAGS := -DCMAKE_TARGET_MESSAGES=OFF

# proto
PROTOBUF_DIR := $(THIRDPARTY_DIR)/protobuf
PROTOC := $(shell which protoc)
PROTOC_GEN_GO := $(GOPATH)/bin/protoc-gen-go

DEPLOY_TARGET := debug # debug | release

.PHONY: rebuild-protobuf build-protobuf compile-protobuf
rebuild-protobuf:
	cd $(PROTOBUF_DIR) && ./autogen.sh && ./configure && make uninstall && make clean && make -j $(NPROC) install

build-protobuf:
ifeq ($(PROTOC),)
	make rebuild-protobuf
else
	@echo 'protoc is already built. Run make rebuild-protobuf to remove existing one and rebuild it.'
endif

$(PROTOC_GEN_GO):
	go install github.com/golang/protobuf/protoc-gen-go

PROTOFILE_DIR := $(abspath proto)

compile-protobuf: $(PROTOC_GEN_GO) 
	rm -f $(PROTOFILE_DIR)/*.go
	protoc --proto_path=$(PROTOFILE_DIR) --go_out=$(PROTOFILE_DIR) $(PROTOFILE_DIR)/*.proto

.PHONY: rebuild-rocksdb build-rocksdb
rebuild-rocksdb:
	rm -rf $(ROCKSDB_BUILD_DIR)
	mkdir -p $(ROCKSDB_BUILD_DIR)
ifdef mac-os-host
	cd $(ROCKSDB_BUILD_DIR) && cmake .. $(BASE_CMAKE_FLAGS) -DCMAKE_BUILD_TYPE=RelWithDebInfo -DPORTABLE=ON -DWITH_TESTS=OFF \
	-DWITH_BENCHMARK_TOOLS=OFF -DWITH_SNAPPY=ON -DUSE_RTTI=ON -DWITH_GFLAGS=OFF \
	&& make -j $(NPROC) install
endif
ifdef linux-os-host
	cd $(ROCKSDB_BUILD_DIR) && cmake .. $(BASE_CMAKE_FLAGS) -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo \
	-DPORTABLE=ON -DWITH_FALLOCATE=OFF -DWITH_TESTS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DWITH_SNAPPY=ON -DUSE_RTTI=ON -DWITH_GFLAGS=OFF \
	&& make -j $(NPROC) install
endif

build-rocksdb:
ifeq ("$(wildcard $(LIB_ROCKSDB))", "")
	make rebuild-rocksdb
else
	@echo RocksDB is already built. Run make rebuild-rocksdb to remove existing one and rebuild it.
endif

.PHONY: build-broker build-client
build-broker:
ifdef linux-os-host
	CGO_ENABLED=1 CGO_CFLAGS="-I$(PWD)/_thirdparty/rocksdb/include" \
	CGO_LDFLAGS="-L$(PWD)/_thirdparty/rocksdb/build -lrocksdb -lstdc++ -lm -lsnappy -ldl" \
	GOOS=linux GOARCH=amd64 \
	go build -tags ${DEPLOY_TARGET} -o ./${BROKER_BIN_DIR}/${BROKER_BIN_NAME} ./${BROKER_BIN_DIR}...
endif
ifdef mac-os-host
	go build -tags ${DEPLOY_TARGET} -o ./${BROKER_BIN_DIR}/${BROKER_BIN_NAME} ./${BROKER_BIN_DIR}...
endif

build-client:
	go build -tags ${DEPLOY_TARGET} -o ./${CLIENT_BIN_DIR}/${CLIENT_BIN_NAME} ./${CLIENT_BIN_DIR}...

.PHONY: build rebuild install test
build:
	if [ -d $(GIT_DIR) ]; then \
		git submodule update --init --recursive; \
	fi
	LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
	make build-protobuf
	make compile-protobuf
	make build-rocksdb
	rm -rf thirdparty/protobuf/examples && go mod tidy
	make build-broker
	make build-client
	make install-config

rebuild:
	if [ -d $(GIT_DIR) ]; then \
		git submodule update --init --recursive; \
	fi
	make rebuild-protobuf
	make compile-protobuf
	make rebuild-rocksdb

install-config:
	mkdir -p ${INSTALL_BROKER_CONFIG_DIR}
	mkdir -p ${INSTALL_ADMIN_CONFIG_DIR}
	mkdir -p ${INSTALL_PRODUCER_CONFIG_DIR}
	mkdir -p ${INSTALL_CONSUMER_CONFIG_DIR}

	cp ${BROKER_CONFIG_DIR}/${CONFIG_NAME}.yml ${INSTALL_BROKER_CONFIG_DIR}
	cp ${ADMIN_CONFIG_DIR}/${CONFIG_NAME}.yml ${INSTALL_ADMIN_CONFIG_DIR}
	cp ${PRODUCER_CONFIG_DIR}/${CONFIG_NAME}.yml ${INSTALL_PRODUCER_CONFIG_DIR}
	cp ${CONSUMER_CONFIG_DIR}/${CONFIG_NAME}.yml ${INSTALL_CONSUMER_CONFIG_DIR}

install:
	cp ${BROKER_BIN_DIR}/${BROKER_BIN_NAME} ${INSTALL_BIN_DIR}/
	cp ${CLIENT_BIN_DIR}/${CLIENT_BIN_NAME} ${INSTALL_BIN_DIR}/

clean:
	rm -f ${BROKER_BIN_DIR}/${BROKER_BIN_NAME}
	rm -f ${CLIENT_BIN_DIR}/${CLIENT_BIN_NAME}
	rm -f ${INSTALL_PATH}/${BROKER_BIN_NAME}
	rm -f ${INSTALL_PATH}/${CLIENT_BIN_NAME}
	rm -rf vendor
	rm -rf .vendor-new
	rm -f $(PROTOFILE_DIR)/*.go
	if [ -d $(ROCKSDB_BUILD_DIR) ]; then \
		cd $(ROCKSDB_BUILD_DIR) && make clean; \
	fi
	rm -rf $(ROCKSDB_BUILD_DIR)
	rm -rf $(INSTALL_CONFIG_HOME_DIR)

test:
	@go test -v
