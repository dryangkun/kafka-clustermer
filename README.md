#Kafka Clustermer
a simple high level consumer running on multi nodes fetch kafka in the same topics.

#Why To Use

##Simply Consume The Same Topics On Multi Nodes
If we want to use a high level consumer running on multi nodes, We need use stream cluster system like storm/spark streaming.

But sometimes we just want simply to consume the same topics on multi nodes, You can use Kafka Clustermer.

##Better High Level API
Using kafka high level consumer, You can't control the offset's commit, And it fetch data then you consume data in different threads.

Using Kafka Clustermer, You can control the offset's commit, And it fetch data then you consume in the same thread.

##More Offset Storage
Kafka high level consumer just support zookeeper and kafka offset storage, 
But Kafka Clustermer support zookeeper, redis, and more simple [chronic-map](https://github.com/OpenHFT/Chronicle-Map).

##Support Storm
[KafkaSpout](./src/main/java/com/github/dryangkun/kafka/clustermer/storm/KafkaSpout.java), it can consume multi topics than [storm-kafka](http://repo1.maven.org/maven2/org/apache/storm/storm-kafka/0.9.5/)


#How To Use

eg: 

    topic:                  test1
    number of partitions:   4
    broker list:            172.16.1.100:9092,172.16.1.101:9092
    consumer nodes:         172.16.1.102,172.16.1.103

##Static Coordinator
Each process on multi nodes consume the different partitions within the same topics, And the different partitions are coordinated by config.

see [StaticExample](./src/test/java/com/github/dryangkun/kafka/clustermer/examples/StaticExample.java)

##Dynamic Coordinator
Each process on multi nodes consume the different partitions within the same topics, Each process is configured the total nodes and the index of the node.

Then then dynamic coordinator calculate the partitions in each process.

see [DynamicExample](./src/test/java/com/github/dryangkun/kafka/clustermer/examples/DynamicExample.java)

