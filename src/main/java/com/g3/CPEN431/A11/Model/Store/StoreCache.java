package com.g3.CPEN431.A11.Model.Store;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;

public class StoreCache {
    private final Cache<ByteBuffer, Message.Msg> cache;
    private final static StoreCache instance = new StoreCache();
    private static final int TIME_OUT = 1; // Assume a message propagation time = 1 second
    public static final int DEFAULT_CACHE_SIZE = 300;

    private StoreCache() {
        cache = Caffeine.newBuilder()
                .maximumSize(DEFAULT_CACHE_SIZE)
                .executor(cleanUpExec())
                .expireAfterWrite(TIME_OUT, TimeUnit.SECONDS)
                .build();
    }

    public static StoreCache getInstance() {
        return instance;
    }
    public Cache<ByteBuffer, Message.Msg> getCache() {
        return cache;
    }

    public  void clearCache () {
        this.cache.invalidateAll();
        this.cache.cleanUp();
        this.cache.policy().eviction().ifPresent(eviction -> eviction.setMaximum(DEFAULT_CACHE_SIZE));
    }
    private ScheduledExecutorService cleanUpExec() {
        ScheduledExecutorService maintenanceService = Executors.newScheduledThreadPool(1);
        maintenanceService.scheduleAtFixedRate(() -> {
            cache.cleanUp();
        }, 0, 5, TimeUnit.SECONDS);
        return maintenanceService;
    }
}
