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


#How To Use

eg: 

    topic:                  test1
    number of partitions:   4
    broker list:            172.16.1.100:9092,172.16.1.101:9092
    consumer nodes:         172.16.1.102,172.16.1.103

##Static Coordinator
Each process on multi nodes consume the different partitions within the same topics, And the different partitions are coordinated by config.

code:

```java
StaticCoordinator coordinator = new StaticCoordinator()
                .addTopicParts("test1", 0, 1);
              //.addTopicParts("test1", 2, 3);
                
ChronicleStorageBuilder storageBuilder = new ChronicleStorageBuilder()
                .addEndpoints("172.16.1.103:9000")
              //.addEndpoints("172.16.1.102:9000")
                .setDataDir("/tmp/xxoo")
                .setId(1)
              //.setId(2)
                .setPort(9000);

ClusterConfig config = new ClusterConfig()
        .setConcurrency(2)
        .setMetaBrokers("172.16.1.100:9092,172.16.1.101:9092")
        .setFetcherMode(FetcherMode.STORAGE_OR_EARLIEST)
        .setCoordinator(coordinator)
        .setStorageBuilder(storageBuilder);
ClusterConsumer consumer = new ClusterConsumer(config);
consumer.start();

Collection<FetcherContainer> containers = consumer.getFetcherContainers();
ExecutorService service = Executors.newFixedThreadPool(containers.size());

for (final FetcherContainer container : containers) {
    service.submit(new Runnable() {
        public void run() {
            try {
                for (int i = 0; i < 2; i++) {
                    Collection<Fetcher> fetchers = container.getFetchers();
                    for (Fetcher fetcher : fetchers) {
                        if (!fetcher.isBroken()) {
                            ByteBufferMessageSet messageSet = fetcher.fetch();
                            for (MessageAndOffset messageAndOffset : messageSet) {
                                System.out.println(messageAndOffset.nextOffset() + "\t"
                                        + new String(Fetcher.getBytes(messageAndOffset)));
                                fetcher.mark(messageAndOffset);
                            }
                            fetcher.commit();
                            fetcher.broken();
                            container.getClusterConsumer().refresh(fetcher);
                            Thread.sleep(1000);
                        }
                    }
                
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            container.closeAllFetchers();
        }
    });
}

while (!service.awaitTermination(1L, TimeUnit.SECONDS));
consumer.close();
```

##Dynamic Coordinator
Each process on multi nodes consume the different partitions within the same topics, Each process is configured the total nodes and the index of the node.

Then then dynamic coordinator calculate the partitions in each process.

```java
DynamicCoordinator coordinator = new DynamicCoordinator()
                .setTopics("test1")
                .setIndexAndTotal(0, 2);
              //.setIndexAndTotal(1, 2);
//same with static coordinator
```