# release-v0.1.0-benchmark
## Benchmark
* Kafka v2.5.0
* Kafka-Go-Client v1.4.2

## Condition
* Consumers consume data and the producer produces data at the same time. (Live data)
* Since ShapleQ does not support batch yet, producer publishes only one twitter chatter data on single request.
* The producer publishes data asynchronously with at-least-once semantics.
* Consumers consumes data with at-least-once semantics.


## Control Variables
* Single Broker
* Single topic
* Single partition
* Replication factor : 1
* Single producer
* 10,000 dataset
## Independent Variables
* The number of consumers

## Dependent Variables
* Elapsed time - Difference between the timestamp that producer starts to produce and the timestamp that all of consumers consumed all dataset.

## Result
* Since few features exist in ShapleQ on this release, the response of the publish request arrives faster than Kafka. Therefore, the elapsed time of ShapleQ going to increase in later releases.

<img width="415" alt="스크린샷 2020-08-31 오후 4 20 32" src="https://user-images.githubusercontent.com/44288167/91693416-f6256b00-eba5-11ea-8e6a-f63f7bd4b736.png">
