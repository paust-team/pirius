# release-v0.1.1-benchmark
## Benchmark
* Kafka v2.5.0
* Kafka-Go-Client v1.4.2

## Condition
* Consumers consume data and the producer produces data at the same time. (Live data)
* Since ShapleQ does not support batch yet, producer publishes only one twitter chatter data on single request.
* The producer publishes data asynchronously with at-least-once semantics.
* Consumers consume data with at-least-once semantics.


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
* Since several goroutines bring about memory leaks on v0.1.0 are fixed, this version of ShapleQ can utilize resources better. As a result, total elapsed time decreased.
