package com.s42442146.CPEN431.A4.model;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import ca.NetSysLab.ProtocolBuffers.Value;
import ca.NetSysLab.ProtocolBuffers.Message;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;


public class KVServer {
    public static final int MAX_KEY_LENGTH = 32; // bytes
    public static final int MAX_VALUE_LENGTH = 10000; // bytes
    public static final int TIME_OUT = 1; // seconds
    public static final int CACHE_SIZE = 100;
    public static final int MAX_MEMORY = 64;  //MB


    private final ExecutorService exec = Executors.newCachedThreadPool();
    private DatagramSocket socket = null;
    public static ConcurrentHashMap<ByteBuffer, Value.Val> KVStore = null;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private HashMap<ByteBuffer, Value.Val> Store = null;


    private Cache<ByteBuffer, Message.Msg> cache =
            CacheBuilder.newBuilder()
                    .maximumSize(CACHE_SIZE)
                    .expireAfterWrite(TIME_OUT, TimeUnit.SECONDS)
                    .build();

    public KVServer(int port) {
        try {
            socket = new DatagramSocket(port);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        KVStore = new ConcurrentHashMap<>();
        Store = new HashMap<>();
    }

    public void start () {
        while (!exec.isShutdown()) {
            byte[] buf = new byte[15000];
            // receive request
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(packet);
//                new Handler(socket, packet, cache, Store).run();
//                exec.submit(new KVServerHandler(socket, packet, cache, KVStore, lock));

                Thread thread = new Thread(new KVServerHandler(socket, packet, cache, KVStore, lock));
                thread.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void stop() {
        exec.shutdown();
        socket.close();
    }
}
