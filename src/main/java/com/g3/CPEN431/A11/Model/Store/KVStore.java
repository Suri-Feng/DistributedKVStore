package com.g3.CPEN431.A11.Model.Store;

import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class KVStore {
    private ConcurrentHashMap<ByteString, Value> store;

    private final static KVStore instance = new KVStore();

    private KVStore() {
        store = new ConcurrentHashMap<>();
    }

    public static KVStore getInstance() {
        return instance;
    }

    public ConcurrentHashMap<ByteString, Value> getStore() {
        return store;
    }

    public  void clearStore () {
        this.store.clear();
        this.store = new ConcurrentHashMap<>();
    }
}
