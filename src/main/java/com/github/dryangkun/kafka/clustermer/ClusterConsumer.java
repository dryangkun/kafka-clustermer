package com.github.dryangkun.kafka.clustermer;

import com.github.dryangkun.kafka.clustermer.coordinator.Coordinator;
import com.github.dryangkun.kafka.clustermer.storage.Storage;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class ClusterConsumer {

    private static final Logger LOG = Logger.getLogger(ClusterConsumer.class);

    private final Connections connections;
    private final Set<Broker> metaBrokers;

    private final Storage storage;
    protected final Coordinator coordinator;

    protected final List<FetcherContainer> fetcherContainers;
    private final FetcherConfig fetcherConfig;

    private final MetaRefresh metaRefresh;

    public ClusterConsumer(ClusterConfig config) throws Exception {
        connections = new Connections(config);
        metaBrokers = new HashSet<Broker>(config.getMetaBrokers());

        storage = config.getStorageBuilder().newStorage();
        coordinator = config.getCoordinator();

        fetcherContainers = new ArrayList<FetcherContainer>(config.getConcurrency());
        for (int i = 0; i < config.getConcurrency(); i++) {
            fetcherContainers.add(new FetcherContainer(i, this));
        }
        fetcherConfig = new FetcherConfig(storage, config);

        MetaRefresh.Factory metaRefreshFactory = config.getMetaRrefreshFactory();
        if (metaRefreshFactory == null) {
            metaRefreshFactory = new AsyncMetaRefresh.Factory();
        }
        metaRefresh = metaRefreshFactory.newRefresher(config.getMetaInterval(), this);
    }

    public synchronized void start() throws Exception {
        metaRefresh.start();
    }

    public FetcherConfig getFetcherConfig() {
        return fetcherConfig;
    }

    public Connections getConnections() {
        return connections;
    }

    public Fetcher newFetcher(Broker broker, Partition partition) {
        return new Fetcher(broker, partition, fetcherConfig);
    }

    public Map<Partition, Broker> findPartBrokers(List<String> topics) throws IOException {
        Set<Broker> newMetaBrokers = new HashSet<Broker>();
        Map<Partition, Broker> partBrokers = null;
        String error = null;

        for (Broker metaBroker : metaBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = connections.getConsumer(metaBroker);
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
                connections.returnConsumer(metaBroker, consumer);
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

    public synchronized void close() {
        metaRefresh.close();
        storage.close();
    }

    public synchronized Collection<FetcherContainer> getFetcherContainers() {
        return fetcherContainers;
    }

    public MetaRefresh getMetaRefresh() {
        return metaRefresh;
    }
}
