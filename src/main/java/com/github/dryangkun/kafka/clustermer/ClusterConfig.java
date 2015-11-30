package com.github.dryangkun.kafka.clustermer;

import com.github.dryangkun.kafka.clustermer.coordinator.Coordinator;
import com.github.dryangkun.kafka.clustermer.storage.StorageBuilder;
import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class ClusterConfig implements Serializable {

    private int soTimeout = 30 * 1000;
    private int bufferSize = 64 * 1024;
    private String clientId;

    private int fetchSize = 1024 * 1024;
    private int maxWait = 100;
    private int minBytes = 1;

    private Set<Broker> metaBrokers = new TreeSet<Broker>();
    private int concurrency = 1;
    private int metaInterval = 60;
    private MetaRefresh.Factory metaRrefreshFactory;

    private FetcherMode fetcherMode = FetcherMode.STORAGE_OR_LATEST;

    public int getSoTimeout() {
        return soTimeout;
    }

    /**
     * socket timeout to kafka simple consumer socket, default 0, never timeout
     * @param soTimeout socket timeout
     * @return this
     */
    public ClusterConfig setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
        return this;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * @param bufferSize socket buffer size, default 2m
     * @return this
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
     * @param clientId consumer clientid
     * @return this
     */
    public ClusterConfig setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * @param fetchSize simple consumer fetch bytes per request
     * @return this
     */
    public ClusterConfig setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public int getMaxWait() {
        return maxWait;
    }

    /**
     * @param maxWait simple consumer fetch wait max microseconds
     * @return this
     */
    public ClusterConfig setMaxWait(int maxWait) {
        this.maxWait = maxWait;
        return this;
    }

    public int getMinBytes() {
        return minBytes;
    }

    /**
     * @param minBytes simple consumer fetch min bytes
     * @return this
     */
    public ClusterConfig setMinBytes(int minBytes) {
        this.minBytes = minBytes;
        return this;
    }

    /**
     * must
     * @param metaBrokers fetch topics meta info broker list
     * @return this
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

    /**
     * must
     * @param brokerList host1:port1,host2:port2
     * @return this
     */
    public ClusterConfig setMetaBrokers(String brokerList) {
        String[] items = brokerList.split(",");
        Set<Broker> brokers = new HashSet<Broker>(items.length);

        for (String item : items) {
            String[] hostPort = item.split(":");
            if (hostPort.length == 1) {
                brokers.add(new Broker(hostPort[0]));
            } else {
                String host = hostPort[0];
                int port = Integer.parseInt(hostPort[1]);
                brokers.add(new Broker(host, port));
            }
        }
        return setMetaBrokers(brokers);
    }

    public Set<Broker> getMetaBrokers() {
        return metaBrokers;
    }

    public int getConcurrency() {
        return concurrency;
    }

    /**
     * @param concurrency number of max threads to consume partitions in current process, default 1
     * @return this
     */
    public ClusterConfig setConcurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    public int getMetaInterval() {
        return metaInterval;
    }

    /**
     * @param metaInterval seconds of refresh topics meta info
     * @return this
     */
    public ClusterConfig setMetaInterval(int metaInterval) {
        this.metaInterval = metaInterval;
        return this;
    }

    public FetcherMode getFetcherMode() {
        return fetcherMode;
    }

    /**
     * @param fetcherMode fetcher init offset mode
     * @return this
     */
    public ClusterConfig setFetcherMode(FetcherMode fetcherMode) {
        this.fetcherMode = fetcherMode;
        return this;
    }

    public MetaRefresh.Factory getMetaRrefreshFactory() {
        return metaRrefreshFactory;
    }

    public ClusterConfig setMetaRrefreshFactory(MetaRefresh.Factory metaRrefreshFactory) {
        this.metaRrefreshFactory = metaRrefreshFactory;
        return this;
    }
}
