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

<img width="419" alt="result" src="https://user-images.githubusercontent.com/44288167/91693216-a646a400-eba5-11ea-9f71-fc5d4e50ab00.png">
