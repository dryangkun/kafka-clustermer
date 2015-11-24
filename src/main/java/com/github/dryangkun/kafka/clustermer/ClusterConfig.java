package com.github.dryangkun.kafka.clustermer;

import com.github.dryangkun.kafka.clustermer.coordinator.Coordinator;
import com.github.dryangkun.kafka.clustermer.storage.Storage;
import com.github.dryangkun.kafka.clustermer.storage.StorageBuilder;
import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

public class ClusterConfig {

    private int soTimeout = 0;
    private int bufferSize = 2 * 1024 * 1024;
    private String clientId;

    private int fetchSize = 2048;
    private int maxWait = FetchRequest.DefaultMaxWait();
    private int minBytes = FetchRequest.DefaultMinBytes();

    private Set<Broker> metaBrokers = new TreeSet<Broker>();
    private int concurrency = 1;
    private int metaInterval = 60;

    private Coordinator coordinator;
    private StorageBuilder storageBuilder;

    private FetcherMode fetcherMode = FetcherMode.STORAGE_OR_LATEST;

    public int getSoTimeout() {
        return soTimeout;
    }

    /**
     * socket timeout to kafka simple consumer socket, default 0, never timeout
     * @param soTimeout
     * @return
     */
    public ClusterConfig setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
        return this;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * socket buffer size, default 2m
     * @param bufferSize
     * @return
     */
    public ClusterConfig setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public String getClientId() {
        if (clientId == null) {
            clientId = OffsetRequest.DefaultClientId();
        }
        return clientId;
    }

    /**
     * set consumer clientid
     * @return
     */
    public ClusterConfig setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * simple consumer fetch bytes per request
     * @param fetchSize
     * @return
     */
    public ClusterConfig setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public int getMaxWait() {
        return maxWait;
    }

    /**
     * simple consumer fetch wait max microseconds
     * @param maxWait
     * @return
     */
    public ClusterConfig setMaxWait(int maxWait) {
        this.maxWait = maxWait;
        return this;
    }

    public int getMinBytes() {
        return minBytes;
    }

    /**
     * simple consumer fetch min bytes
     * @param minBytes
     * @return
     */
    public ClusterConfig setMinBytes(int minBytes) {
        this.minBytes = minBytes;
        return this;
    }

    /**
     * must
     * fetch topics meta info broker list
     * @param metaBrokers
     * @return
     */
    public ClusterConfig setMetaBrokers(Broker... metaBrokers) {
        Collections.addAll(this.metaBrokers, metaBrokers);
        return this;
    }

    /**
     * @see ClusterConfig#setMetaBrokers(Broker...)
     */
    public ClusterConfig setMetaBrokers(Set<Broker> metaBrokers) {
        this.metaBrokers = metaBrokers;
        return this;
    }

    public Set<Broker> getMetaBrokers() {
        return metaBrokers;
    }

    public int getConcurrency() {
        return concurrency;
    }

    /**
     * number of max threads to consume partitions in current process, default 1
     * @param concurrency
     * @return
     */
    public ClusterConfig setConcurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    public int getMetaInterval() {
        return metaInterval;
    }

    /**
     * seconds of refresh topics meta info
     * @param metaInterval
     * @return
     */
    public ClusterConfig setMetaInterval(int metaInterval) {
        this.metaInterval = metaInterval;
        return this;
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }

    public ClusterConfig setCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
        return this;
    }

    public StorageBuilder getStorageBuilder() {
        return storageBuilder;
    }

    public ClusterConfig setStorageBuilder(StorageBuilder storageBuilder) {
        this.storageBuilder = storageBuilder;
        this.storageBuilder.setClusterConfig(this);
        return this;
    }

    public FetcherMode getFetcherMode() {
        return fetcherMode;
    }

    /**
     * fetcher init offset mode
     * @param fetcherMode
     * @return
     */
    public ClusterConfig setFetcherMode(FetcherMode fetcherMode) {
        this.fetcherMode = fetcherMode;
        return this;
    }
}
