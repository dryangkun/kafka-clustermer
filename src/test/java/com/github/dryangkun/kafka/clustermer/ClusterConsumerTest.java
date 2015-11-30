package com.github.dryangkun.kafka.clustermer;

import com.github.dryangkun.kafka.clustermer.coordinator.StaticCoordinator;
import com.github.dryangkun.kafka.clustermer.storage.*;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClusterConsumerTest {

    public static ClusterConfig newClusterConfig() {
        return new ClusterConfig()
                .setConcurrency(1)
                .setMetaBrokers("10.6.7.218:9092")
                .setFetcherMode(FetcherMode.STORAGE_OR_EARLIEST)
                .setFetchSize(29);
    }

    private <A extends Storage> void testConsumer(ClusterConfig config, Storage storage) throws Exception {
        StaticCoordinator coordinator = new StaticCoordinator()
                .addTopicParts("test1", 0);

        ClusterConsumer consumer = new ClusterConsumer(config, coordinator, storage);
        consumer.start();

        Collection<FetcherContainer> containers = consumer.getFetcherContainers();
        ExecutorService service = Executors.newFixedThreadPool(containers.size());

        for (final FetcherContainer container : containers) {
            service.submit(new Runnable() {
                public void run() {
                    try {
                        for (int i = 0; i < 2; i++) {
                            System.out.println("loop:" + i);
                            Collection<Fetcher> fetchers = container.getFetchers();
                            for (Fetcher fetcher : fetchers) {
                                ByteBufferMessageSet messageSet = fetcher.fetch();
                                for (MessageAndOffset messageAndOffset : messageSet) {
                                    System.out.println(messageAndOffset.nextOffset() + "\t"
                                            + new String(Fetcher.getBytes(messageAndOffset)));
                                    fetcher.mark(messageAndOffset);
                                }
                                fetcher.commit();
                                container.refreshFetcher(fetcher);
                                Thread.sleep(1000);
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

    @Test public void testChronic() throws Exception {
        System.out.println("test chronic");
        ClusterConfig config = newClusterConfig();

        ChronicleStorage storage = new ChronicleStorageBuilder(config)
                //.addEndpoint("127.0.0.1", 9002)
                .setDataDir("/tmp/xxoo")
                .setId(1)
                .setPort(9001)
                .newStorage();
        storage.getMap().clear();
        testConsumer(config, storage);
    }

    @Test public void testRedis() throws Exception {
        System.out.println("test redis");
        ClusterConfig config = newClusterConfig();

        RedisStorage storage = new RedisStorageBuilder(config)
                .setHost("localhost")
                .setKey("xxoo")
                .newStorage();

        Jedis jedis = storage.getJedisPool().getResource();
        jedis.del(storage.getKey());
        storage.getJedisPool().returnResource(jedis);
        testConsumer(config, storage);
    }

    @Test public void testZookeeper() throws Exception {
        System.out.println("test zookeeper");
        ClusterConfig config = newClusterConfig();

        ZookeeperStorage storage = new ZookeeperStorageBuilder(config)
                .setConnectString("10.6.7.218:2181")
                .setGroupId("xxoo")
                .newStorage();

        if (storage.getClient().checkExists().forPath(storage.getPathPrefix()) != null) {
            storage.getClient().delete()
                    .deletingChildrenIfNeeded()
                    .forPath(storage.getPathPrefix());
        }
        testConsumer(config, storage);
    }
}
