package com.github.dryangkun.kafka.clustermer.storage;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Compatible for high-level consumer,
 * Save offset to path "{zkRoot}/{groupId}/offsets/{topic}/{partitionId}"
 */
public class ZookeeperStorageBuilder extends StorageBuilder<ZookeeperStorage> {

    private String zkRoot = "/";
    private String groupId;

    private String connectString;
    private Integer sessionTimeoutMs = null;
    private Integer connectionTimeoutMs = null;
    private RetryPolicy retryPolicy;

    /**
     * @param zkRoot
     * @return
     */
    public ZookeeperStorageBuilder setZkRoot(String zkRoot) {
        if (zkRoot == null || zkRoot.length() == 0) {
            throw new IllegalArgumentException("zkRoot is empty");
        }
        if (!zkRoot.startsWith("/")) {
            throw new IllegalArgumentException("zkRoot not start with '/'=" + zkRoot);
        }
        if (zkRoot.length() > 1 && zkRoot.endsWith("/")) {
            zkRoot = zkRoot.substring(0, zkRoot.length() - 1);
        }
        this.zkRoot = zkRoot;
        return this;
    }

    /**
     * must
     * set consumer group id
     * @param groupId
     * @return
     */
    public ZookeeperStorageBuilder setGroupId(String groupId) {
        if (groupId.contains("/")) {
            throw new IllegalArgumentException("groupId contains '/'=" + groupId);
        }
        this.groupId = groupId;
        return this;
    }

    /**
     * must
     * set zookeeper connect address, eg:
     * @param connectString
     * @return
     */
    public ZookeeperStorageBuilder setConnectString(String connectString) {
        this.connectString = connectString;
        return this;
    }

    /**
     * set session and connection timeout ms
     * @param sessionTimeoutMs
     * @param connectionTimeoutMs
     * @return
     */
    public ZookeeperStorageBuilder setTimeoutMs(int sessionTimeoutMs, int connectionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        return this;
    }

    /**
     * set retry policy default ExponentialBackoffRetry(200, 3)
     * @param retryPolicy
     * @return
     */
    public ZookeeperStorageBuilder setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    @Override
    public ZookeeperStorage newStorage() throws Exception {
        if (retryPolicy == null) {
            retryPolicy = new ExponentialBackoffRetry(200, 3);
        }
        CuratorFramework client;
        if (sessionTimeoutMs == null) {
            client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        } else {
            client = CuratorFrameworkFactory.newClient(connectString,
                    sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        }
        return new ZookeeperStorage(
                (zkRoot.length() == 1 ? zkRoot : zkRoot + "/") + groupId + "/offsets",
                client);
    }
}
