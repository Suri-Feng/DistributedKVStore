package com.s42442146.CPEN431.A4.model;

import ca.NetSysLab.ProtocolBuffers.Value;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class KVStore {
    private ConcurrentHashMap<ByteBuffer, Value.Val> store;
    private final static KVStore instance = new KVStore();

    private KVStore() {
        store = new ConcurrentHashMap<>();
    }

    public static KVStore getInstance() {
        return instance;
    }

    public ConcurrentHashMap<ByteBuffer, Value.Val> getStore() {
        return store;
    }

    public  void clearStore () {
        this.store.clear();
        this.store = new ConcurrentHashMap<>();
        Runtime.getRuntime().freeMemory();
        System.gc();
    }
}
