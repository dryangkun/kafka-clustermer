package com.github.dryangkun.kafka.clustermer.storage;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * chronic-map is the map that different processes can share,
 * detail see http://chronicle.software/products/chronicle-map/
 */
public class ChronicleStorageBuilder extends StorageBuilder<ChronicleStorage> {

    private int port = 8076;
    private List<InetSocketAddress> endpoints = new ArrayList<InetSocketAddress>();
    private int heartbeat = 3;

    private File dataDir;
    private byte id;

    private TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig;
    private ChronicleMapBuilder<String, Long> chronicleMapBuilder;

    /**
     * must
     * chronic map in current consumer process use socket to communicate with others
     * @param port
     * @return
     */
    public ChronicleStorageBuilder setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * must
     * other consumer processes that consume different partitions
     * @param address
     * @return
     */
    public ChronicleStorageBuilder addEndpoint(InetSocketAddress address) {
        endpoints.add(address);
        return this;
    }

    /**
     * @see ChronicleStorageBuilder#addEndpoint(InetSocketAddress)
     */
    public ChronicleStorageBuilder addEndpoint(String host, int port) {
        endpoints.add(new InetSocketAddress(host, port));
        return this;
    }

    /**
     * chronic map processes heartbeat to each other interval seconds
     * @param heartbeat
     * @return
     */
    public ChronicleStorageBuilder setHeartbeat(int heartbeat) {
        this.heartbeat = heartbeat;
        return this;
    }

    /**
     * must
     * each consumer process save offset data directory
     * @param dataDir
     * @return
     */
    public ChronicleStorageBuilder setDataDir(File dataDir) {
        if (dataDir.exists()) {
            if (!dataDir.isDirectory()) {
                throw new IllegalArgumentException("data dir is not a direcoty=" + dataDir);
            }
        } else if (!dataDir.mkdirs()) {
            throw new IllegalArgumentException("data dir mkdirs fail=" + dataDir);
        }

        this.dataDir = dataDir;
        return this;
    }

    /**
     * @see ChronicleStorageBuilder#setDataDir(File)
     */
    public ChronicleStorageBuilder setDataDir(String dataDir) {
        return setDataDir(new File(dataDir));
    }

    public File getDataFile() {
        return new File(dataDir, "kafka-clustermer-" + id + ".offset");
    }

    /**
     * must
     * chronic map id that different from others
     * @param id
     * @return
     */
    public ChronicleStorageBuilder setId(byte id) {
        this.id = id;
        return this;
    }

    /**
     * @see ChronicleStorageBuilder#setId(byte)
     */
    public ChronicleStorageBuilder setId(int id) {
        if (id < -128 || id > 255) {
            throw new IllegalArgumentException("id can't convert to a byte");
        }
        this.id = (byte) id;
        return this;
    }

    /**
     * maybe user want to config other options, so can use this method to get object
     * @return
     */
    public TcpTransportAndNetworkConfig getTcpTransportAndNetworkConfig() {
        if (tcpTransportAndNetworkConfig == null) {
            if (port <= 0) {
                throw new IllegalArgumentException("port invalid=" + port);
            }

            tcpTransportAndNetworkConfig = TcpTransportAndNetworkConfig
                    .of(port, endpoints)
                    .heartBeatInterval(heartbeat, TimeUnit.SECONDS);
        }
        return tcpTransportAndNetworkConfig;
    }

    protected void clearTcpTransportAndNetworkConfig() {
        tcpTransportAndNetworkConfig = null;
    }

    /**
     * maybe user want to config other options, so can use this method to get object
     * @return
     */
    public ChronicleMapBuilder<String, Long> getChronicleMapBuilder() {
        if (chronicleMapBuilder == null) {
            chronicleMapBuilder = ChronicleMapBuilder
                    .of(String.class, Long.class)
                    .replication(id, getTcpTransportAndNetworkConfig());
        }
        return chronicleMapBuilder;
    }

    protected void clearChronicleMapBuilder() {
        chronicleMapBuilder = null;
    }

    public ChronicleStorage newStorage() throws Exception {
        ChronicleMapBuilder<String, Long> chronicleMapBuilder = getChronicleMapBuilder();
        File dataFile = getDataFile();
        if (!dataFile.exists() && !dataFile.createNewFile()) {
            throw new IOException("");
        }

        final ChronicleMap<String, Long> map = chronicleMapBuilder.createPersistedTo(dataFile);

        clearTcpTransportAndNetworkConfig();
        clearChronicleMapBuilder();

        return new ChronicleStorage(map);
    }
}
