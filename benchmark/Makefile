SQ_BROKER_BENCH_DIR := shapleQ/broker
SQ_PRODUCER_BENCH_DIR := shapleQ/client/producer
SQ_CONSUMER_BENCH_DIR := shapleQ/client/consumer
SQ_TOPIC_ADMIN_DIR := shapleQ/client/admin

SQ_BROKER_NAME := sq-broker-bench
SQ_PRODUCER_NAME := sq-producer-bench
SQ_CONSUMER_NAME := sq-consumer-bench
SQ_TOPIC_ADMIN_NAME := sq-topic

KF_BROKER_BENCH_DIR := kafka/broker
KF_PRODUCER_BENCH_DIR := kafka/client/producer
KF_CONSUMER_BENCH_DIR := kafka/client/consumer
KF_TOPIC_ADMIN_DIR := kafka/client/admin

KF_BROKER_NAME := kf-broker-bench
KF_PRODUCER_NAME := kf-producer-bench
KF_CONSUMER_NAME := kf-consumer-bench
KF_TOPIC_ADMIN_NAME := kf-topic

BENCH_INSTALL_HOME_DIR := ${HOME}/.shapleq/bench

KF_DATA_DIR := ${KF_BROKER_BENCH_DIR}/kafka-data

build:
	mkdir -p ${BENCH_INSTALL_HOME_DIR}

	# Build shapleQ binaries
	go build -o ./${SQ_BROKER_BENCH_DIR}/${SQ_BROKER_NAME} ./${SQ_BROKER_BENCH_DIR}...
	go build -o ./${SQ_PRODUCER_BENCH_DIR}/${SQ_PRODUCER_NAME} ./${SQ_PRODUCER_BENCH_DIR}...
	go build -o ./${SQ_CONSUMER_BENCH_DIR}/${SQ_CONSUMER_NAME} ./${SQ_CONSUMER_BENCH_DIR}...
	go build -o ./${SQ_TOPIC_ADMIN_DIR}/${SQ_TOPIC_ADMIN_NAME} ./${SQ_TOPIC_ADMIN_DIR}...

	# Build kafka binaries
	go build -tags dynamic -o ./${KF_PRODUCER_BENCH_DIR}/${KF_PRODUCER_NAME} ./${KF_PRODUCER_BENCH_DIR}...
	go build -tags dynamic -o ./${KF_CONSUMER_BENCH_DIR}/${KF_CONSUMER_NAME} ./${KF_CONSUMER_BENCH_DIR}...
	go build -tags dynamic -o ./${KF_TOPIC_ADMIN_DIR}/${KF_TOPIC_ADMIN_NAME} ./${KF_TOPIC_ADMIN_DIR}...

	# Build clean_zk.go
	go build clean_zk.go

clean:
	- ./clean_zk
	- rm clean_zk \
	${SQ_BROKER_BENCH_DIR}/${SQ_BROKER_NAME} \
	${SQ_PRODUCER_BENCH_DIR}/${SQ_PRODUCER_NAME} \
	${SQ_CONSUMER_BENCH_DIR}/${SQ_CONSUMER_NAME} \
	${SQ_TOPIC_ADMIN_DIR}/${SQ_TOPIC_ADMIN_NAME} \
	${KF_PRODUCER_BENCH_DIR}/${KF_PRODUCER_NAME} \
	${KF_CONSUMER_BENCH_DIR}/${KF_CONSUMER_NAME} \
	${KF_TOPIC_ADMIN_DIR}/${KF_TOPIC_ADMIN_NAME}
	- zkCli -server "localhost:2181" deleteall /kf-debug
	rm -rf ${KF_DATA_DIR}
