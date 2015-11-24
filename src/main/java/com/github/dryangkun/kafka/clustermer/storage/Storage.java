package com.github.dryangkun.kafka.clustermer.storage;

import com.github.dryangkun.kafka.clustermer.Partition;

/**
 * thread-safe
 */
public interface Storage {
    /**
     * get offset by partition
     * @param partition
     * @return
     * @throws Exception
     */
    long get(Partition partition) throws Exception;

    /**
     * save offset
     * @param partition
     * @param offset
     * @throws Exception
     */
    void put(Partition partition, long offset) throws Exception;

    /**
     * close
     */
    void close();
}
