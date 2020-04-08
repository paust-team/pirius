GOPATH	:= $(shell go env GOPATH)
GIT_DIR := $(shell git rev-parse --git-dir 2>/dev/null || true)
NPROC := $(shell nproc)
THIRDPARTY_DIR := $(abspath thirdparty)

PROTOBUF_DIR := $(THIRDPARTY_DIR)/protobuf
ROCKSDB_DIR := $(THIRDPARTY_DIR)/rocksdb
ROCKSDB_BUILD_DIR := $(ROCKSDB_DIR)/build
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
	rebuild-protobuf
else
	@echo 'protoc is already built. Run make rebuild-protobuf to remove existing one and rebuild it.'
endif

$(PROTOC_GEN_GO):
	go get -u github.com/golang/protobuf/protoc-gen-go

PROTOFILE_DIR := $(abspath proto)

$(PROTOFILE_DIR)/data.pb.go: $(PROTOFILE_DIR)/data.proto | $(PROTOC_GEN_GO) $(PROTOC)
	protoc --proto_path=$(PROTOFILE_DIR) --go_out=$(PROTOFILE_DIR) $(PROTOFILE_DIR)/data.proto
$(PROTOFILE_DIR)/api.pb.go: $(PROTOFILE_DIR)/api.proto | $(PROTOC_GEN_GO) $(PROTOC)
	protoc --proto_path=$(PROTOFILE_DIR) --go_out=$(PROTOFILE_DIR) $(PROTOFILE_DIR)/api.proto

compile-protobuf: $(PROTOFILE_DIR)/data.pb.go $(PROTOFILE_DIR)/api.pb.go

.PHONY: rebuild-rocksdb build-rocksdb
rebuild-rocksdb:
	cd $(ROCKSDB_BUILD_DIR) && make clean
	rm -rf $(ROCKSDB_BUILD_DIR)
	mkdir -p $(ROCKSDB_BUILD_DIR)
ifdef mac-os-host
	cd $(ROCKSDB_BUILD_DIR) && cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DPORTABLE=ON -DWITH_TESTS=OFF \
	-DWITH_BENCHMARK_TOOLS=OFF -DWITH_SNAPPY=ON -DUSE_RTTI=ON -DWITH_GFLAGS=OFF -DCMAKE_INSTALL_PREFIX=$(THIRDPARTY_DIR)\
	&& make -j $(NPROC) install
endif
ifdef linux-os-host
	cd $(ROCKSDB_BUILD_DIR) && cmake .. $(BASE_CMAKE_FLAGS) -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo \
	-DPORTABLE=ON -DWITH_FALLOCATE=OFF -DWITH_TESTS=OFF -DWITH_SNAPPY=ON -DUSE_RTTI=ON -DWITH_GFLAGS=OFF \
	&& make -j $(NPROC) install
endif

build-rocksdb:
ifeq ("$(wildcard $(LIB_ROCKSDB))", "")
	make rebuild-rocksdb
else
	@echo RocksDB is already built. Run make rebuild-rocksdb to remove existing one and rebuild it.
endif

.PHONY: build rebuild test
build:

	if test -n $(GIT_DIR); then \
		git submodule update --init --recursive; \
	fi
	make build-protobuf
	make compile-protobuf
	make build-rocksdb
rebuild:
	if test -n $(GIT_DIR); then \
		git submodule update --init --recursive; \
	fi
	make rebuild-protobuf
	make compile-protobuf
	make rebuild-rocksdb
test:
	@go test -v
