package com.g3.CPEN431.A11.Model.Store;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A11.Model.Distribution.NodesCircle;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.protobuf.ByteString;

public class StoreCache {
    private final Cache<ByteString, Message.Msg> cache;
    private Cache<ByteString, QueuedMessage> queuedResponses;
    private final static StoreCache instance = new StoreCache();
    private static final int TIME_OUT = 1; // Assume a message propagation time = 1 second
    public static final int DEFAULT_CACHE_SIZE = 500;
    NodesCircle nodesCircle = NodesCircle.getInstance();

    private StoreCache() {
        if (nodesCircle.getStartupNodesSize() == 1) {
            cache = Caffeine.newBuilder()
                    .maximumSize(DEFAULT_CACHE_SIZE)
                    .executor(cleanUpExec())
                    .expireAfterWrite(TIME_OUT, TimeUnit.SECONDS)
                    .build();
        } else {
            cache = Caffeine.newBuilder()
                    .maximumSize(DEFAULT_CACHE_SIZE)
                    .expireAfterWrite(TIME_OUT, TimeUnit.SECONDS)
                    .build();
        }

        queuedResponses = Caffeine.newBuilder()
                .expireAfterWrite(4, TimeUnit.SECONDS)
                .build();
    }

    public static StoreCache getInstance() {
        return instance;
    }

    public Cache<ByteString, Message.Msg> getCache() {
        return cache;
    }

    public Cache<ByteString, QueuedMessage> getQueuedResponses() {
        return queuedResponses;
    }


    public void clearCache() {
        this.cache.invalidateAll();
        this.cache.cleanUp();
        this.queuedResponses.invalidateAll();
        this.queuedResponses.cleanUp();
        if (nodesCircle.getStartupNodesSize() == 1) {
            this.cache.policy().eviction().ifPresent(eviction -> eviction.setMaximum(DEFAULT_CACHE_SIZE));
        }
    }

    private ScheduledExecutorService cleanUpExec() {
        ScheduledExecutorService maintenanceService = Executors.newScheduledThreadPool(1);
        maintenanceService.scheduleAtFixedRate(() -> {
            cache.cleanUp();
        }, 0, 5, TimeUnit.SECONDS);
        return maintenanceService;
    }
}
