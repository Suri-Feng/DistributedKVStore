package com.s42442146.CPEN431.A4.model.Distribution;

import java.util.concurrent.ConcurrentHashMap;

public class NodesMap {

    private ConcurrentHashMap<Integer, Node> nodesTable;

    private final static NodesMap instance = new NodesMap();

    private NodesMap() {
        nodesTable = new ConcurrentHashMap<>();
    }
    public static NodesMap getInstance() {
        return instance;
    }

    public ConcurrentHashMap<Integer, Node> getNodesTable() {
        return nodesTable;
    }
}
