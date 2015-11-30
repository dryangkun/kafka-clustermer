package com.github.dryangkun.kafka.clustermer;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public abstract class MetaRefresh {

    private final static Logger LOG = Logger.getLogger(MetaRefresh.class);

    protected final int metaInterval;
    protected final Set<Broker> metaBrokers;
    protected final ClusterConsumer clusterConsumer;

    public MetaRefresh(int metaInterval, Set<Broker> metaBrokers, ClusterConsumer clusterConsumer) {
        this.metaInterval = metaInterval;
        this.metaBrokers = metaBrokers;
        this.clusterConsumer = clusterConsumer;
    }

    public abstract void start() throws Exception;

    public abstract boolean refresh() throws Exception;

    public abstract void refresh(Fetcher oldFetcher) throws Exception;

    public abstract void close();

    public interface Factory<A extends MetaRefresh> extends Serializable {
        A newRefresher(int metaInterval, Set<Broker> metaBrokers, ClusterConsumer clusterConsumer);
    }

    public Map<Partition, Broker> findPartBrokers(List<String> topics) throws IOException {
        Set<Broker> newMetaBrokers = new HashSet<Broker>();
        Map<Partition, Broker> partBrokers = null;
        String error = null;

        for (Broker metaBroker : metaBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = clusterConsumer.connections.getConsumer(metaBroker);
                TopicMetadataRequest request = new TopicMetadataRequest(topics);
                TopicMetadataResponse response = consumer.send(request);

                List<TopicMetadata> metadatas = response.topicsMetadata();
                Map<Partition, Broker> _partBrokers = new TreeMap<Partition, Broker>();

                for (TopicMetadata metadata : metadatas) {
                    for (PartitionMetadata partitionMetadata : metadata.partitionsMetadata()) {
                        Partition partition = new Partition(
                                metadata.topic(), partitionMetadata.partitionId());
                        Broker broker = new Broker(partitionMetadata.leader());
                        _partBrokers.put(partition, broker);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("find partition=" + partition + " at broker=" + broker);
                        }

                        if (!metaBrokers.contains(broker)) {
                            LOG.info("find new meta broker=" + broker);
                            newMetaBrokers.add(broker);
                        }
                    }
                }
                partBrokers = _partBrokers;
                break;
            } catch (Exception e) {
                LOG.warn("topic meta data request fail=" + topics + " at broker=" + metaBroker, e);
                error = e.getMessage();
            } finally {
                clusterConsumer.connections.returnConsumer(metaBroker, consumer);
            }
        }

        metaBrokers.addAll(newMetaBrokers);
        if (partBrokers != null) {
            return partBrokers;
        } else {
            throw new IOException("meta request fail=" + error + " for topics=" + topics);
        }
    }

    protected Map<Partition, Broker> findPartBrokers(String... topics) throws IOException {
        return findPartBrokers(Arrays.asList(topics));
    }
}
