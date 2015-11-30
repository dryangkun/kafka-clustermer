package com.github.dryangkun.kafka.clustermer.storage;

import com.github.dryangkun.kafka.clustermer.ClusterConfig;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * chronic-map is the map that different processes can share,
 * detail see http://chronicle.software/products/chronicle-map/
 */
public class ChronicleStorageBuilder extends StorageBuilder<ChronicleStorage> implements Serializable {

    private int port = 8076;
    private List<InetSocketAddress> endpoints = new ArrayList<InetSocketAddress>();
    private int heartbeat = 3;

    private File dataDir;
    private byte id;

    private TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig;
    private ChronicleMapBuilder<String, Long> chronicleMapBuilder;

    public ChronicleStorageBuilder(ClusterConfig clusterConfig) {
        super(clusterConfig);
    }

    /**
     * must
     * @param port port to communicate with others, default 8076
     * @return this
     */
    public ChronicleStorageBuilder setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * must
     * @param address other consumer processes that consume different partitions
     * @return this
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
     * must
     * @param hostList host1:port1,host2:port2
     * @return this
     */
    public ChronicleStorageBuilder addEndpoints(String hostList) {
        String[] items = hostList.split(",");
        for (String item : items) {
            String[] hostPort = item.split(":");

            String host = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);
            endpoints.add(new InetSocketAddress(host, port));
        }
        return this;
    }

    /**
     * @param heartbeat heartbeat to each other interval seconds
     * @return this
     */
    public ChronicleStorageBuilder setHeartbeat(int heartbeat) {
        this.heartbeat = heartbeat;
        return this;
    }

    /**
     * must
     * @param dataDir save offset data directory
     * @return this
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
     * @param id id that different from others
     * @return this
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
     * @return TcpTransportAndNetworkConfig
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
     * @return ChronicleMapBuilder
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
