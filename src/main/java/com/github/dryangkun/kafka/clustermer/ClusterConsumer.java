package com.github.dryangkun.kafka.clustermer;

import com.github.dryangkun.kafka.clustermer.coordinator.Coordinator;
import com.github.dryangkun.kafka.clustermer.storage.Storage;
import com.github.dryangkun.kafka.clustermer.storage.StorageBuilder;
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

    protected final Connections connections;

    protected final Storage storage;
    protected final Coordinator coordinator;
    protected final FetcherConfig fetcherConfig;
    protected final MetaRefresh metaRefresh;

    protected final List<FetcherContainer> fetcherContainers;

    public ClusterConsumer(ClusterConfig config, Coordinator coordinator, Storage storage) throws Exception {
        connections = new Connections(config);
        this.coordinator = coordinator;
        this.storage = storage;

        fetcherContainers = new ArrayList<FetcherContainer>(config.getConcurrency());
        for (int i = 0; i < config.getConcurrency(); i++) {
            fetcherContainers.add(new FetcherContainer(i, this));
        }
        fetcherConfig = new FetcherConfig(storage, config);

        MetaRefresh.Factory metaRefreshFactory = config.getMetaRrefreshFactory();
        if (metaRefreshFactory == null) {
            metaRefreshFactory = new AsyncMetaRefresh.Factory();
        }
        Set<Broker> metaBrokers = new HashSet<Broker>(config.getMetaBrokers());
        metaRefresh = metaRefreshFactory.newRefresher(config.getMetaInterval(), metaBrokers, this);
    }

    public synchronized void start() throws Exception {
        metaRefresh.start();
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
