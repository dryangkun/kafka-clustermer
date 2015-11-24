package com.github.dryangkun.kafka.clustermer.storm;

import backtype.storm.Config;
import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;
import com.github.dryangkun.kafka.clustermer.*;
import com.github.dryangkun.kafka.clustermer.coordinator.DynamicCoordinator;
import com.github.dryangkun.kafka.clustermer.storage.StorageBuilder;
import com.github.dryangkun.kafka.clustermer.storage.ZookeeperStorageBuilder;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import storm.kafka.FailedFetchException;
import storm.kafka.KafkaConfig;
import storm.kafka.KeyValueSchemeAsMultiScheme;

import java.nio.ByteBuffer;
import java.util.*;

public class KafkaSpout extends BaseRichSpout {

    public static class MessageAndRealOffset {
        public Message msg;
        public long offset;

        public MessageAndRealOffset(Message msg, long offset) {
            this.msg = msg;
            this.offset = offset;
        }
    }

    private final SpoutConfig config;

    private ClusterConsumer clusterConsumer;
    private FetcherContainer fetcherContainer;
    private SpoutOutputCollector collector;

    public KafkaSpout(SpoutConfig config) {
        this.config = config;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(config.scheme.getOutputFields());
    }

    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        DynamicCoordinator coordinator = new DynamicCoordinator()
                .setIndexAndTotal(context.getThisTaskIndex(), totalTasks)
                .setTopics(config.topics);
        config.clusterConfig.setCoordinator(coordinator);

        StorageBuilder storageBuilder = config.clusterConfig.getStorageBuilder();
        if (storageBuilder instanceof ZookeeperStorageBuilder) {
            ZookeeperStorageBuilder zookeeperStorageBuilder = (ZookeeperStorageBuilder) storageBuilder;

            if (zookeeperStorageBuilder.isEmptyConnectString()) {
                String connectString = null;
                {
                    List<String> hosts = (List<String>) map.get(Config.STORM_ZOOKEEPER_SERVERS);
                    int port = ((Number) map.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
                    StringBuilder builder = new StringBuilder();
                    for (String host : hosts) {
                        builder.append(host).append(":").append(port).append(",");
                    }
                    connectString = builder.substring(0, -1);
                }
                zookeeperStorageBuilder.setConnectString(connectString);
            }
        }
        config.clusterConfig.setConcurrency(1);
        config.clusterConfig.setMetaRrefreshFactory(new SyncMetaRefresh.Factory());
        try {
            clusterConsumer = new ClusterConsumer(config.clusterConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Collection<FetcherContainer> fetcherContainers = clusterConsumer.getFetcherContainers();
        fetcherContainer = fetcherContainers.iterator().next();
    }

    private Iterator<Fetcher> iterator;
    private LinkedList<MessageAndRealOffset> pendings = new LinkedList<MessageAndRealOffset>();

    public void nextTuple() {
        if (pendings.isEmpty()) {
            if (iterator == null || iterator.hasNext()) {
                iterator = fetcherContainer.getFetchers().iterator();
            }

            Fetcher fetcher = iterator.next();
            try {
                ByteBufferMessageSet messageSet = fetcher.fetch();
                for (MessageAndOffset msg : messageSet) {
                    pendings.add(new MessageAndRealOffset(msg.message(), msg.offset()));
                }
                if (!pendings.isEmpty()) {
                    fetcher.commit(pendings.getLast().offset);
                }
            } catch (KafkaException e) {
                //todo refresh fetcher
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        MessageAndRealOffset messageAndOffset = pendings.pollFirst();
        if (messageAndOffset != null) {
            Iterable<List<Object>> tups = generateTuples(config.scheme, messageAndOffset.msg);
            if (tups != null) {
                for (List<Object> tup : tups) {
                    collector.emit(tup);
                }
            }
        }
    }

    public static Iterable<List<Object>> generateTuples(MultiScheme scheme, Message msg) {
        Iterable<List<Object>> tups;
        ByteBuffer payload = msg.payload();
        if (payload == null) {
            return null;
        }
        ByteBuffer key = msg.key();
        if (key != null && scheme instanceof KeyValueSchemeAsMultiScheme) {
            tups = ((KeyValueSchemeAsMultiScheme) scheme).deserializeKeyAndValue(Utils.toByteArray(key), Utils.toByteArray(payload));
        } else {
            tups = scheme.deserialize(Utils.toByteArray(payload));
        }
        return tups;
    }

    @Override
    public void close() {
        fetcherContainer.closeAllFetchers();
        clusterConsumer.close();
    }
}
