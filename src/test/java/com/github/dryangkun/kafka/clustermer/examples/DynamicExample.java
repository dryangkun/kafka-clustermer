package com.github.dryangkun.kafka.clustermer.examples;

import com.github.dryangkun.kafka.clustermer.*;
import com.github.dryangkun.kafka.clustermer.coordinator.DynamicCoordinator;
import com.github.dryangkun.kafka.clustermer.coordinator.StaticCoordinator;
import com.github.dryangkun.kafka.clustermer.storage.ChronicleStorageBuilder;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DynamicExample {

    /**
     * example environment
     * topic:                       test1,test2
     * number of test1 partitions:  4
     * number of test2 partitions:  4
     * broker list:                 172.16.1.100:9092,172.16.1.101:9092
     * consumer nodes:              172.16.1.102,172.16.1.103
     */
    public static void main(String[] args) throws Exception {
        //run at 172.16.1.102 node
        DynamicCoordinator coordinator = new DynamicCoordinator()
                .setIndexAndTotal(0, 2)
//              72.16.1.103 node set like that:
//              .setIndexAndTotal(1, 2)
                .setTopics("test1", "test2");

        ClusterConfig config = new ClusterConfig()
                .setConcurrency(2)
                .setMetaBrokers("172.16.1.100:9092,172.16.1.101:9092")
                .setFetcherMode(FetcherMode.STORAGE_OR_EARLIEST);

        ChronicleStorageBuilder storageBuilder = new ChronicleStorageBuilder(config)
                .addEndpoints("172.16.1.103:9000")
//              .addEndpoints("172.16.1.102:9000")
                .setDataDir("/tmp/clustermer-group1-test1_test2")
                .setId(1)
//              .setId(2)
                .setPort(9000);

        ClusterConsumer consumer = new ClusterConsumer(
                config, coordinator, storageBuilder.newStorage());
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
                                ByteBufferMessageSet messageSet = fetcher.fetch();
                                for (MessageAndOffset messageAndOffset : messageSet) {
                                    System.out.println(messageAndOffset.nextOffset() + "\t"
                                            + new String(Fetcher.getBytes(messageAndOffset)));
                                    fetcher.mark(messageAndOffset);
                                }
                                fetcher.commit();
                            }

                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    container.closeAllFetchers();
                }
            });
        }

        service.shutdown();
        while (!service.awaitTermination(1L, TimeUnit.SECONDS));
        consumer.close();

    }
}
