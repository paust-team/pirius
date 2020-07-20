GOPATH	:= $(shell go env GOPATH)
GIT_DIR := $(shell git rev-parse --git-dir 2>/dev/null || true)
NPROC := $(shell nproc)
THIRDPARTY_DIR := $(abspath thirdparty)

PROTOBUF_DIR := $(THIRDPARTY_DIR)/protobuf
ROCKSDB_DIR := $(THIRDPARTY_DIR)/rocksdb
ROCKSDB_BUILD_DIR := $(ROCKSDB_DIR)/build
ROCKSDB_INCLUDE_DIR := $(ROCKSDB_DIR)/include
LIB_ROCKSDB := $(ROCKSDB_BUILD_DIR)/librocksdb.a

BASE_CMAKE_FLAGS := -DCMAKE_TARGET_MESSAGES=OFF

PROTOC := $(shell which protoc)
PROTOC_GEN_GO := $(GOPATH)/bin/protoc-gen-go

mac-os-host := $(findstring Darwin, $(shell uname))
linux-os-host := $(findstring Linux, $(shell uname))

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
	go get -u github.com/golang/protobuf/protoc-gen-go

PROTOFILE_DIR := $(abspath proto)

compile-protobuf: $(PROTOC_GEN_GO) 
	rm -f $(PROTOFILE_DIR)/*.go
	protoc --proto_path=$(PROTOFILE_DIR) --go_out=$(PROTOFILE_DIR) $(PROTOFILE_DIR)/*.proto

.PHONY: rebuild-rocksdb build-rocksdb
rebuild-rocksdb:
	if [ -d $(ROCKSDB_BUILD_DIR) ]; then \
		cd $(ROCKSDB_BUILD_DIR) && make clean; \
	fi
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
	cd broker/cmd/shapleq && CGO_ENABLED=1 CGO_CFLAGS="-I/go/src/github.com/paust-team/shapleq/thirdparty/rocksdb/include" CGO_LDFLAGS="-L/go/src/github.com/paust-team/shapleq/thirdparty/rocksdb/build -lrocksdb -lstdc++ -lm -lsnappy -ldl" GOOS=linux GOARCH=amd64 go build
endif
ifdef mac-os-host
	cd broker/cmd/shapleq && go build
endif

build-client:
	cd client/cmd/shapleq-client && go build

.PHONY: build rebuild install test
build:
	if [ -d $(GIT_DIR) ]; then \
		git submodule update --init --recursive; \
	fi
	make build-protobuf
	make compile-protobuf
	make build-rocksdb
	dep ensure
	make build-broker
	make build-client
rebuild:
	if [ -d $(GIT_DIR) ]; then \
		git submodule update --init --recursive; \
	fi
	make rebuild-protobuf
	make compile-protobuf
	make rebuild-rocksdb
install:
	cp broker/cmd/shapleq/shapleq /usr/local/bin/
	cp client/cmd/shapleq-client/shapleq-client /usr/local/bin/
clean:
	rm -f /usr/local/bin/shapleq
	rm -f /usr/local/bin/shapleq-client
	rm -rf vendor
	rm -rf .vendor-new
	rm -f $(PROTOFILE_DIR)/*.go
	if [ -d $(ROCKSDB_BUILD_DIR) ]; then \
		cd $(ROCKSDB_BUILD_DIR) && make clean; \
	fi
	rm -rf $(ROCKSDB_BUILD_DIR)

test:
	@go test -v
