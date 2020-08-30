SQ_BROKER_BENCH_DIR := shapleQ/broker
SQ_BROKER_NAME := sq-broker-bench
SQ_CLEAN_ZK_DIR := ${SQ_BROKER_BENCH_DIR}/clean_zk
SQ_CLEAN_ZK_NAME := clean_zk

KF_BROKER_BENCH_DIR := kafka/broker

BENCH_CLIENT_BINARY := benchmark

BENCH_INSTALL_HOME_DIR := ${HOME}/.shapleq/bench

KF_DATA_DIR := ${KF_BROKER_BENCH_DIR}/kafka-data

build-broker:
	mkdir -p ${BENCH_INSTALL_HOME_DIR}
	go build -o ./${SQ_BROKER_BENCH_DIR}/${SQ_BROKER_NAME} ./${SQ_BROKER_BENCH_DIR}/bench_broker.go
	go build -o ./${SQ_CLEAN_ZK_DIR}/${SQ_CLEAN_ZK_NAME} ./${SQ_CLEAN_ZK_DIR}...

build-bench:
	go build -tags dynamic -o ${BENCH_CLIENT_BINARY} ./benchmark.go

build:
	make build-broker
	make build-bench

clean:
	- ./${SQ_CLEAN_ZK_DIR}/${SQ_CLEAN_ZK_NAME}
	- rm ${SQ_CLEAN_ZK_DIR}/${SQ_CLEAN_ZK_NAME} \
	${SQ_BROKER_BENCH_DIR}/${SQ_BROKER_NAME} \
	${BENCH_CLIENT_BINARY}
	make clean-data

clean-data:
	- zkCli -server "localhost:2181" deleteall /kf-debug
	- rm -rf ${KF_DATA_DIR}
	- rm -rf ${BENCH_INSTALL_HOME_DIR}
	- ./${SQ_CLEAN_ZK_DIR}/${SQ_CLEAN_ZK_NAME}
	- rm *result.txt

clean-result:
	- rm *result.txt