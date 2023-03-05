package com.g3.CPEN431.A7.Model.Store;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class HintedStore {

    private final static HintedStore instance = new HintedStore();

    private HintedStore() {
        hintedNodeToMap = new ConcurrentHashMap<>();
    }

    public static HintedStore getInstance() {
        return instance;
    }

    public ConcurrentHashMap<Integer, ConcurrentHashMap<ByteBuffer, ValueV>> getStore() {
        return hintedNodeToMap;
    }

    public ConcurrentHashMap<Integer, ConcurrentHashMap<ByteBuffer, ValueV>> hintedNodeToMap;

    public  void clearStoreByNodeId (int id) {
        this.hintedNodeToMap.remove(id);
    }
}
