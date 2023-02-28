package com.s42442146.CPEN431.A4.model;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.concurrent.*;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.s42442146.CPEN431.A4.model.Distribution.EpidemicServer;
import com.s42442146.CPEN431.A4.model.Distribution.Node;
import com.s42442146.CPEN431.A4.model.Distribution.NodesCircle;
import com.s42442146.CPEN431.A4.model.Store.StoreCache;

public class KVServer {
    public static final int MAX_KEY_LENGTH = 32; // bytes
    public static final int MAX_VALUE_LENGTH = 10000; // bytes
    private static final int DEFAULT_CACHE_SIZE = StoreCache.DEFAULT_CACHE_SIZE;
    private long currentCacheSize = DEFAULT_CACHE_SIZE;
    private static final int THREAD_POOL_SIZE = 5;
    private final StoreCache storeCache = StoreCache.getInstance();
    private final DatagramSocket socket;
    private final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public KVServer(int port) {
        try {
            socket = new DatagramSocket(port);
            String ip = InetAddress.getLocalHost().getHostAddress() + port;
            NodesCircle nodesCircle = NodesCircle.getInstance();
            Node node = nodesCircle.getNodeFromIp(ip);

            // Make sure the current node is in the nodes list
            if (node != null) {
                System.out.println("Server running on port: " + node.getPort());
                nodesCircle.setThisNodeRingHash(nodesCircle.getCircleHashFromNodeHash(node.getHash()));
                nodesCircle.setThisNodeId(node.getId());

                // start epidemic server
                EpidemicServer server = new EpidemicServer(socket, node.getId());
                ScheduledExecutorService epidemicService = Executors.newScheduledThreadPool(1);
                epidemicService.scheduleAtFixedRate(
                        server, 0, 10, TimeUnit.MILLISECONDS);
            } else {
                System.out.println(InetAddress.getLocalHost().getHostAddress());
                System.exit(0);
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
            byte[] buf = new byte[15000];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            try {
                socket.receive(packet);
                Message.Msg requestMessage = Message.Msg.parseFrom
                        (Arrays.copyOfRange(buf, 0, packet.getLength()));

                pool.execute(new KVServerHandler(requestMessage,
                        socket, packet.getAddress(), packet.getPort()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
