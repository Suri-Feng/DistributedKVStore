package com.g3.CPEN431.A9.Model.Distribution;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;

public class PredecessorCache {
    private final Cache<Integer, Boolean> cache;
    private final static PredecessorCache instance = new PredecessorCache();
    private static final int TIME_OUT = 30; // Assume a message propagation time = 1 second
    public static final int DEFAULT_CACHE_SIZE = 5;

    private PredecessorCache() {
        cache = Caffeine.newBuilder()
                .maximumSize(DEFAULT_CACHE_SIZE)
                .expireAfterWrite(TIME_OUT, TimeUnit.SECONDS)
                .build();
    }

    public static PredecessorCache getInstance() {
        return instance;
    }
    public Cache<Integer, Boolean> getCache() {
        return cache;
    }
}
