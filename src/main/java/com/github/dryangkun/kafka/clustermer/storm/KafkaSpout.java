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
import com.github.dryangkun.kafka.clustermer.storage.ZookeeperStorageBuilder;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KeyValueSchemeAsMultiScheme;

import java.nio.ByteBuffer;
import java.util.*;

public class KafkaSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    public static class MessageAndRealOffset {
        public Message msg;
        public Partition part;
        public long offset;

        public MessageAndRealOffset(Message msg, Partition part, long offset) {
            this.msg = msg;
            this.part = part;
            this.offset = offset;
        }
    }

    private final SpoutConfig spoutConfig;

    private ClusterConsumer clusterConsumer;
    private FetcherContainer fetcherContainer;
    private SpoutOutputCollector collector;

    public KafkaSpout(SpoutConfig config) {
        this.spoutConfig = config;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(spoutConfig.scheme.getOutputFields());
    }

    @SuppressWarnings("unchecked")
    public static String getStormZkConnectString(Map conf) {
        List<String> hosts = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        int port = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        StringBuilder builder = new StringBuilder();
        for (String host : hosts) {
            builder.append(host).append(":").append(port).append(",");
        }
        return builder.substring(0, builder.length() - 1);
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        DynamicCoordinator coordinator = new DynamicCoordinator()
                .setIndexAndTotal(context.getThisTaskIndex(), totalTasks)
                .setTopics(spoutConfig.topics);

        if (spoutConfig.storageBuilder == null) {
            if (spoutConfig.groupId == null || spoutConfig.zkRoot == null) {
                throw new RuntimeException("when SpoutConfig.storageBuilder is null, SpoutConfig.groupId and SpoutConfig.zkRoot should not be null");
            }

            ZookeeperStorageBuilder storageBuilder = new ZookeeperStorageBuilder(spoutConfig.clusterConfig);
            storageBuilder.setConnectString(getStormZkConnectString(conf))
                    .setGroupId(spoutConfig.groupId)
                    .setZkRoot(spoutConfig.zkRoot);
        } else if (spoutConfig.storageBuilder instanceof ZookeeperStorageBuilder) {
            ZookeeperStorageBuilder storageBuilder = (ZookeeperStorageBuilder) spoutConfig.storageBuilder;
            if (storageBuilder.isEmptyConnectString()) {
                storageBuilder.setConnectString(getStormZkConnectString(conf));
            }
        }

        spoutConfig.clusterConfig.setConcurrency(1);
        spoutConfig.clusterConfig.setMetaRrefreshFactory(new SyncMetaRefresh.Factory());
        try {
            clusterConsumer = new ClusterConsumer(
                    spoutConfig.clusterConfig, coordinator,
                    spoutConfig.storageBuilder.newStorage());
            clusterConsumer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Collection<FetcherContainer> fetcherContainers = clusterConsumer.getFetcherContainers();
        fetcherContainer = fetcherContainers.iterator().next();
    }

    private Iterator<Fetcher> iterator;
    private LinkedList<MessageAndRealOffset> pendings = new LinkedList<MessageAndRealOffset>();

    private int _emitCount = 0;
    private long _lastCommitTime = -1;
    private Map<Partition, Long> partOffsets = new HashMap<Partition, Long>();

    private void fill() {
        if (iterator == null || !iterator.hasNext()) {
            iterator = fetcherContainer.getFetchers().iterator();
        }
        while (iterator.hasNext()) {
            Fetcher fetcher = iterator.next();
            try {
                ByteBufferMessageSet messageSet = fetcher.fetch();
                for (MessageAndOffset msg : messageSet) {
                    pendings.add(new MessageAndRealOffset(
                            msg.message(), fetcher.getPart(), msg.offset()));
                    fetcher.mark(msg.offset());
                }
                if (pendings.size() >= spoutConfig.pendingMaxSize) {
                    break;
                }
            } catch (KafkaException e) {
                if (e.needRefresh()) {
                    LOG.error("get data from fetcher=" + fetcher + " fail, then refresh fetcher", e);
                    fetcherContainer.refreshFetcher(fetcher);
                } else {
                    LOG.error("get data from fetcher=" + fetcher + " fail", e);
                }
            } catch (Exception e) {
                LOG.error("get data from fetcher=" + fetcher + " fail", e);
            }
        }
    }

    public void nextTuple() {
        if (pendings.isEmpty()) {
            fill();
        }

        MessageAndRealOffset msg = pendings.pollFirst();
        if (msg != null) {
            Iterable<List<Object>> tups = generateTuples(spoutConfig.scheme, msg.msg);
            if (tups != null) {
                for (List<Object> tup : tups) {
                    collector.emit(tup);
                }
                partOffsets.put(msg.part, msg.offset);
                _emitCount++;
            }
        }

        long now = System.currentTimeMillis();
        if (_emitCount >= spoutConfig.commitPerEmitCount) {
            commit(now);
        } else if (now - _lastCommitTime >= spoutConfig.commitPerIntervalMs) {
            commit(now);
        }

        try {
            if (clusterConsumer.getMetaRefresh().refresh()) {
                iterator = null;
            }
        } catch (Exception e) {
            LOG.error("refresh meta fail", e);
        }
    }

    public void commit(long now) {
        List<Partition> removeParts = new ArrayList<Partition>();
        for (Partition part : partOffsets.keySet()) {
            Long offset = partOffsets.get(part);
            Fetcher fetcher = fetcherContainer.getFetcher(part);

            if (offset == null || fetcher == null) {
                removeParts.add(part);
            } else {
                try {
                    fetcher.storeCommitedOffset(offset);
                } catch (Exception e) {
                    LOG.error("store emited offset fail, fetcher=" + fetcher, e);
                }
            }
        }
        for (Partition part : removeParts) {
            partOffsets.remove(part);
        }
        _emitCount = 0;
        _lastCommitTime = now;
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
    public void deactivate() {
        commit(System.currentTimeMillis());
    }

    @Override
    public void close() {
        fetcherContainer.closeAllFetchers();
        clusterConsumer.close();
    }
}
