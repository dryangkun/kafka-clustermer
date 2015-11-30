package com.github.dryangkun.kafka.clustermer.storage;

import com.github.dryangkun.kafka.clustermer.Fetcher;
import com.github.dryangkun.kafka.clustermer.Partition;
import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class ZookeeperStorage implements Storage {

    private final static Logger LOG = Logger.getLogger(ZookeeperStorage.class);

    private final String pathPrefix;
    private final CuratorFramework client;

    public ZookeeperStorage(String pathPrefix, CuratorFramework client) {
        this.pathPrefix = pathPrefix;
        this.client = client;
        this.client.start();
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public String partToPath(Partition partition) {
        return pathPrefix + "/" + partition.getTopic() + "/" + partition.getId();
    }

    public long get(Partition partition) throws Exception {
        String path = partToPath(partition);
        if (client.checkExists().forPath(path) != null) {
            long offset = toLong(client.getData().forPath(path));
            if (offset >= 0) {
                return offset;
            }
        }
        return Fetcher.EMPTY_OFFSET;
    }

    public void put(Partition partition, long offset) throws Exception {
        String path = partToPath(partition);
        byte[] bytes = toBytes(offset);
        if (client.checkExists().forPath(path) != null) {
            client.setData().forPath(path, bytes);
        } else {
            client.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(path, bytes);
        }
    }

    public void close() {
        LOG.info("closing");
        client.close();
        LOG.info("closed");
    }

    private static byte[] toBytes(long value) {
        byte [] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) value;
            value >>>= 8;
        }
        b[0] = (byte) value;
        return b;
    }

    private static long toLong(byte[] bytes) {
        if (bytes.length != 8) {
            return -1;
        }

        long l = 0;
        for(int i = 0; i < 8; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }
}
