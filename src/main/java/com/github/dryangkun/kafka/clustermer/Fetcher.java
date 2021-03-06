package com.github.dryangkun.kafka.clustermer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * non-thread-safe
 */
public class Fetcher implements Comparable<Fetcher> {

    private static final Logger LOG = Logger.getLogger(Fetcher.class);

    public static final long EMPTY_OFFSET = -1;

    private final Broker broker;
    private final Partition part;
    private final FetcherConfig config;

    private volatile FetcherContainer container;
    private volatile SimpleConsumer consumer;

    private long commitedOffset = EMPTY_OFFSET;
    private long uncommitedOffset = EMPTY_OFFSET;

    private volatile boolean closing = false;

    public Fetcher(Broker broker, Partition partition,
                   FetcherConfig config) {
        this.broker = broker;
        this.part = partition;
        this.config = config;
    }

    public FetcherContainer getContainer() {
        return container;
    }

    public void setContainer(FetcherContainer container) {
        this.container = container;
    }

    public SimpleConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(SimpleConsumer consumer) {
        this.consumer = consumer;
    }

    public Broker getBroker() {
        return broker;
    }

    public Partition getPart() {
        return part;
    }

    public boolean isClosing() {
        return closing;
    }

    public void closing() {
        closing = true;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return compareTo((Fetcher) obj) == 0;
    }

    @Override
    public String toString() {
        return "" + part + "@" + broker;
    }

    public int compareTo(Fetcher fetcher) {
        int i = broker.compareTo(fetcher.broker);
        if (i == 0) {
            return part.compareTo(fetcher.part);
        } else {
            return i;
        }
    }

    public static OffsetResponse getOffset(SimpleConsumer consumer,
                                           Partition part,
                                           long whileTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(part.getTopic(), part.getId());
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition,
                new PartitionOffsetRequestInfo(whileTime, 1));

        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo,
                OffsetRequest.CurrentVersion(), consumer.clientId());
        return consumer.getOffsetsBefore(request);
    }

    /**
     * fetch request kafka partition by the offset,
     * and if request fail, then throw KafkaException
     * else throw Exception
     * @param offset partition offset
     * @return ByteBufferMessageSet
     * @throws Exception
     */
    public ByteBufferMessageSet fetch(long offset) throws Exception {
        FetchRequest request = new FetchRequestBuilder()
                .addFetch(part.getTopic(), part.getId(), offset, config.fetchSize)
                .maxWait(config.maxWait)
                .minBytes(config.minBytes)
                .build();
        FetchResponse response = consumer.fetch(request);
        if (response.hasError()) {
            short errorCode = response.errorCode(part.getTopic(), part.getId());
            throw new KafkaException("fetch request fail", broker, part, errorCode);
        }
        return response.messageSet(part.getTopic(), part.getId());
    }

    /**
     * init commited offset by fetcherConfig.mode
     * @throws Exception
     */
    public void initCommitedOffset() throws Exception {
        if (config.mode.isStorage()) {
            int _try = 3;
            while (_try-- > 0) {
                try {
                    commitedOffset = config.storage.get(part);
                    break;
                } catch (Exception e) {
                    if (_try == 0) {
                        throw e;
                    } else {
                        Thread.sleep(200);
                        LOG.error("get offset from storage fail, partition=" + part, e);
                    }
                }
            }
            if (commitedOffset != EMPTY_OFFSET) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("init offset from storage, partition=" + part + ", offset=" + commitedOffset);
                }
                return;
            }
        }

        long whileTime = config.mode.isEarliest() ?
                OffsetRequest.EarliestTime() :
                OffsetRequest.LatestTime();
        OffsetResponse response = getOffset(consumer, part, whileTime);
        if (response.hasError()) {
            short errorCode = response.errorCode(part.getTopic(), part.getId());
            throw new KafkaException("offset request fail", broker, part, errorCode);
        }
        commitedOffset = response.offsets(part.getTopic(), part.getId())[0];
        if (LOG.isDebugEnabled()) {
            LOG.debug("init offset from kafka, partition=" + part + ", offset=" + commitedOffset + ", is earliest=" + config.mode.isEarliest());
        }
    }

    /**
     * store offset and let commited offset = the offset
     * @param offset storing offset
     * @throws Exception
     */
    public void storeCommitedOffset(long offset) throws Exception {
        int _try = 3;
        while (_try-- > 0) {
            try {
                config.storage.put(part, offset);
                commitedOffset = offset;
                break;
            } catch (Exception e) {
                if (_try == 0) {
                    throw e;
                } else {
                    Thread.sleep(200);
                    LOG.error("put offset to storage fail, partition=" + part + ", offset=" + offset, e);
                }
            }
        }
    }

    /**
     * let uncommited offset = commited offset
     * @throws Exception
     */
    public void reset() throws Exception {
        uncommitedOffset = commitedOffset;
    }

    /**
     * fetch request kafka partition by uncommited offset,
     * and if commited offset is not inited, then call {@link #initCommitedOffset()},
     * and if uncommited offset is not inited, then call {@link #reset()}
     * @see #fetch()
     * @return ByteBufferMessageSet
     * @throws Exception
     */
    public ByteBufferMessageSet fetch() throws Exception {
        if (commitedOffset == EMPTY_OFFSET) {
            initCommitedOffset();
        }
        if (uncommitedOffset == EMPTY_OFFSET) {
            reset();
        }
        return fetch(uncommitedOffset + 1);
    }

    /**
     * let the uncommited offset = the offset
     * @param offset the offset
     */
    public void mark(long offset) {
        uncommitedOffset = offset;
    }

    /**
     * @see #mark(long)
     */
    public void mark(MessageAndOffset messageAndOffset) {
        uncommitedOffset = messageAndOffset.offset();
    }

    /**
     * store uncommited offset
     * @see #storeCommitedOffset(long)
     * @throws Exception
     */
    public void commit() throws Exception {
        if (uncommitedOffset != commitedOffset) {
            storeCommitedOffset(uncommitedOffset);
        }
    }

    public static byte[] getBytes(MessageAndOffset messageAndOffset) {
        ByteBuffer payload = messageAndOffset.message().payload();
        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        return bytes;
    }
}
