package com.s42442146.CPEN431.A4.model;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.s42442146.CPEN431.A4.model.Distribution.Node;
import com.s42442146.CPEN431.A4.model.Distribution.NodesMap;

public class KVServer {
    public static final int MAX_KEY_LENGTH = 32; // bytes
    public static final int MAX_VALUE_LENGTH = 10000; // bytes
    private static final int DEFAULT_CACHE_SIZE = StoreCache.DEFAULT_CACHE_SIZE;
    private long currentCacheSize = DEFAULT_CACHE_SIZE;
    private static final int THREAD_POOL_SIZE = 5;
    private final StoreCache storeCache = StoreCache.getInstance();
    private final NodesMap nodesMap = NodesMap.getInstance();
    private final DatagramSocket socket;
    private final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private List<Node> nodes;
    public static int ownHash = -1;

    public KVServer(int port, ArrayList<Node> nodes) {
        try {
            socket = new DatagramSocket(port);
            this.nodes = nodes;

            int numNodes = nodes.size();
            int n = 1 << numNodes;
            for (Node node: nodes) {
                int hash = (int) (node.getId() % n < 0 ? node.getId() % n + n : node.getId() % n);
                nodesMap.getNodesTable().put(hash, node);
                if (port == node.getPort()
                        && InetAddress.getLocalHost().getHostAddress().equals(node.getAddress().getHostAddress())) {
                    ownHash = hash;
                    System.out.println("Server running on port: " + port);
                }
            }
            if (ownHash == -1) {
                System.out.println(InetAddress.getLocalHost().getHostAddress());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void resizeCache() {
        if (storeCache.getCache().estimatedSize() >= currentCacheSize * 2/ 3) {
            storeCache.getCache().policy().eviction().ifPresent(eviction -> {
                eviction.setMaximum(2 * eviction.getMaximum());
                currentCacheSize = eviction.getMaximum();
            });
        } else if (currentCacheSize != DEFAULT_CACHE_SIZE
                && storeCache.getCache().estimatedSize() < DEFAULT_CACHE_SIZE * 2/ 3) {
            storeCache.getCache().policy().eviction().ifPresent(eviction -> {
                eviction.setMaximum(DEFAULT_CACHE_SIZE);
                currentCacheSize = DEFAULT_CACHE_SIZE;
            });
        }
    }

    public void start() {
        // receive request
        while (true) {
            resizeCache();
            byte[] buf = new byte[13000];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            try {
                socket.receive(packet);
                Message.Msg requestMessage = Message.Msg.parseFrom
                        (Arrays.copyOfRange(buf, 0, packet.getLength()));

                pool.execute(new KVServerHandler(requestMessage,
                        socket, packet.getAddress(), packet.getPort(), nodes));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
