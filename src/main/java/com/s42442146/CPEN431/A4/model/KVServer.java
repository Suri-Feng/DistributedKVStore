package com.s42442146.CPEN431.A4.model;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.*;


import ca.NetSysLab.ProtocolBuffers.Message;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class KVServer {
    public static final int MAX_KEY_LENGTH = 32; // bytes
    public static final int MAX_VALUE_LENGTH = 10000; // bytes
    public static final int TIME_OUT = 2; // Assume a message propagation time = 1 second
    public static final int DEFAULT_CACHE_SIZE = 500;
    private long currentCacheSize = DEFAULT_CACHE_SIZE;
    public static final int THREAD_POOL_SIZE = 5;

    public DatagramSocket socket;
    public final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public final Cache<ByteBuffer, Message.Msg> cache =
            Caffeine.newBuilder()
                    .maximumSize(DEFAULT_CACHE_SIZE)
                    .executor(cleanUpExec())
                    .expireAfterWrite(TIME_OUT, TimeUnit.SECONDS)
                    .build();

    private ScheduledExecutorService cleanUpExec() {
        ScheduledExecutorService maintenanceService = Executors.newScheduledThreadPool(1);
        maintenanceService.scheduleAtFixedRate(() -> {
            cache.cleanUp();
        }, 0, 5, TimeUnit.SECONDS);
        return maintenanceService;
    }

    public KVServer(int port) {
        try {
            socket = new DatagramSocket(port);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

    private void resizeCache() {
        if (cache.estimatedSize() >= currentCacheSize * 2/ 3) {
            cache.policy().eviction().ifPresent(eviction -> {
                eviction.setMaximum(2 * eviction.getMaximum());
                currentCacheSize = eviction.getMaximum();
            });
        } else if (currentCacheSize != DEFAULT_CACHE_SIZE && cache.estimatedSize() < DEFAULT_CACHE_SIZE * 2/ 3) {
            cache.policy().eviction().ifPresent(eviction -> {
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
                        socket, cache, packet.getAddress(), packet.getPort()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
