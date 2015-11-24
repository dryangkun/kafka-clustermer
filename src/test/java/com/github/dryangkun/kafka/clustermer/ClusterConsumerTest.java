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

    private interface StorageInit<A extends Storage> {
        void init(A storage) throws Exception;
    }

    private <A extends Storage> void testConsumer(StorageBuilder storageBuilder,
                                                  StorageInit<A> storageInit) throws Exception {
        StaticCoordinator coordinator = new StaticCoordinator()
                .addTopicParts("test1", 0);

        ClusterConfig config = new ClusterConfig()
                .setConcurrency(1)
                .setMetaBrokers(
                        new Broker("10.6.7.218", 9092)
                )
                .setFetcherMode(FetcherMode.STORAGE_OR_EARLIEST)
                .setFetchSize(29)
                .setCoordinator(coordinator)
                .setStorageBuilder(storageBuilder);
        ClusterConsumer consumer = new ClusterConsumer(config);
        consumer.start();

        storageInit.init((A) consumer.getFetcherConfig().storage);

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
    }

    @Test public void testChronic() throws Exception {
        ChronicleStorageBuilder storageBuilder = new ChronicleStorageBuilder()
                //.addEndpoint("127.0.0.1", 9002)
                .setDataDir("/tmp/xxoo")
                .setId(1)
                .setPort(9001);

        testConsumer(storageBuilder, new StorageInit<ChronicleStorage>() {
            public void init(ChronicleStorage storage) throws Exception {
                storage.getMap().clear();
            }
        });
    }

    @Test public void testRedis() throws Exception {
        RedisStorageBuilder storageBuilder = new RedisStorageBuilder()
                .setHost("localhost")
                .setKey("xxoo");

        testConsumer(storageBuilder, new StorageInit<RedisStorage>() {
            public void init(RedisStorage storage) throws Exception {
                Jedis jedis = storage.getJedisPool().getResource();
                jedis.del(storage.getKey());
                storage.getJedisPool().returnResource(jedis);
            }
        });
    }

    @Test public void testZookeeper() throws Exception {
        ZookeeperStorageBuilder storageBuilder = new ZookeeperStorageBuilder()
                .setConnectString("10.6.7.218:2181")
                .setGroupId("xxoo");

        testConsumer(storageBuilder, new StorageInit<ZookeeperStorage>() {
            public void init(ZookeeperStorage storage) throws Exception {
                if (storage.getClient().checkExists().forPath(storage.getPathPrefix()) != null) {
                    storage.getClient().delete()
                            .deletingChildrenIfNeeded()
                            .forPath(storage.getPathPrefix());
                }
            }
        });
    }
}
