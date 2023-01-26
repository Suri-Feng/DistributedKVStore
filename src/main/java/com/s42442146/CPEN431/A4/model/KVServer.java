package com.s42442146.CPEN431.A4.model;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import ca.NetSysLab.ProtocolBuffers.Message;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class KVServer {
    public static final int MAX_KEY_LENGTH = 32; // bytes
    public static final int MAX_VALUE_LENGTH = 10000; // bytes
    public static final int TIME_OUT = 2; // Assume a message propagation time = 1 second
    public static final int CACHE_SIZE = 500;
    public static final int THREAD_POOL_SIZE = 10;

    public DatagramSocket socket;
    public final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    public final Cache<ByteBuffer, Message.Msg> cache =
            Caffeine.newBuilder()
                    .maximumSize(CACHE_SIZE)
                    .expireAfterWrite(TIME_OUT, TimeUnit.SECONDS)
                    .build();

    public KVServer(int port) {
        try {
            socket = new DatagramSocket(port);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        byte[] buf;
        // receive request
        while (true) {
            buf = new byte[15000];
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
