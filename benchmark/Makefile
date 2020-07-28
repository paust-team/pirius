SQ_BROKER_BENCH_DIR := broker/shapleQ
SQ_PRODUCER_BENCH_DIR := client/shapleQ/producer
SQ_CONSUMER_BENCH_DIR := client/shapleQ/consumer

SQ_BROKER_NAME := sq-broker-bench
SQ_PRODUCER_NAME := sq-producer-bench
SQ_CONSUMER_NAME := sq-consumer-bench

KF_BROKER_BENCH_DIR := broker/kafka
KF_PRODUCER_BENCH_DIR := client/kafka/producer
KF_CONSUMER_BENCH_DIR := client/kafka/consumer

KF_BROKER_NAME := kf-broker-bench
KF_PRODUCER_NAME := kf-producer-bench
KF_CONSUMER_NAME := kf-consumer-bench

BENCH_INSTALL_HOME_DIR := ${HOME}/.shapleq/bench

build:
	mkdir -p ${BENCH_INSTALL_HOME_DIR}

	# Build shapleQ binaries
	go build -o ./${SQ_BROKER_BENCH_DIR}/${SQ_BROKER_NAME} ./${SQ_BROKER_BENCH_DIR}...
	go build -o ./${SQ_PRODUCER_BENCH_DIR}/${SQ_PRODUCER_NAME} ./${SQ_PRODUCER_BENCH_DIR}...
	go build -o ./${SQ_CONSUMER_BENCH_DIR}/${SQ_CONSUMER_NAME} ./${SQ_CONSUMER_BENCH_DIR}...

	# Build kafka binaries

	# Build clean_zk.go
	go build clean_zk.go

clean:
	./clean_zk
	rm clean_zk
	rm ${SQ_BROKER_BENCH_DIR}/${SQ_BROKER_NAME}
	rm ${SQ_PRODUCER_BENCH_DIR}/${SQ_PRODUCER_NAME}
	rm ${SQ_CONSUMER_BENCH_DIR}/${SQ_CONSUMER_NAME}
