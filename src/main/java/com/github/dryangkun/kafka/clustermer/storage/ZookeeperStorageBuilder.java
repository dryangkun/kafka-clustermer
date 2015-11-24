package com.github.dryangkun.kafka.clustermer.storage;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.Serializable;

/**
 * Compatible for high-level consumer,
 * Save offset to path "{zkRoot}/{groupId}/offsets/{topic}/{partitionId}"
 */
public class ZookeeperStorageBuilder extends StorageBuilder<ZookeeperStorage> implements Serializable {

    private String zkRoot = "/";
    private String groupId;

    private String connectString;
    private Integer sessionTimeoutMs = null;
    private Integer connectionTimeoutMs = null;
    private RetryPolicyFactory retryPolicyFactory;

    public interface RetryPolicyFactory extends Serializable {
        RetryPolicy newRetryPolicy();
    }

    /**
     * @param zkRoot zookeeper root path
     * @return this
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
     * @param groupId consumer group id
     * @return this
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
     * @param connectString zookeeper connect address
     * @return this
     */
    public ZookeeperStorageBuilder setConnectString(String connectString) {
        this.connectString = connectString;
        return this;
    }

    /**
     * @param sessionTimeoutMs session timeout ms
     * @param connectionTimeoutMs connection timeout ms
     * @return this
     */
    public ZookeeperStorageBuilder setTimeoutMs(int sessionTimeoutMs, int connectionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        return this;
    }

    /**
     * @param retryPolicyFactory retry policy factory
     * @return this
     */
    public ZookeeperStorageBuilder setRetryPolicy(RetryPolicyFactory retryPolicyFactory) {
        this.retryPolicyFactory = retryPolicyFactory;
        return this;
    }

    @Override
    public ZookeeperStorage newStorage() throws Exception {
        RetryPolicy retryPolicy;

        if (retryPolicyFactory == null) {
            retryPolicy = new ExponentialBackoffRetry(200, 3);
        } else {
            retryPolicy = retryPolicyFactory.newRetryPolicy();
        }
        CuratorFramework client;
        if (sessionTimeoutMs == null) {
            client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        } else {
            client = CuratorFrameworkFactory.newClient(connectString,
                    sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        }
        if (groupId == null) {
            groupId = clusterConfig.getClientId();
        }

        return new ZookeeperStorage(
                (zkRoot.length() == 1 ? zkRoot : zkRoot + "/") + groupId + "/offsets",
                client);
    }

    public boolean isEmptyConnectString() {
        return connectString == null;
    }
}
