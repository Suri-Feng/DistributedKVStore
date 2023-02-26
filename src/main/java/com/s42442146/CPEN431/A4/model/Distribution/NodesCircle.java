package com.s42442146.CPEN431.A4.model.Distribution;

import java.util.concurrent.ConcurrentSkipListMap;

public class NodesCircle {
    private ConcurrentSkipListMap<Integer, Node> nodesTable;
    private final static NodesCircle instance = new NodesCircle();

    private NodesCircle() {
        nodesTable = new ConcurrentSkipListMap<>();
    }
    public static NodesCircle getInstance() {
        return instance;
    }

    public ConcurrentSkipListMap<Integer, Node> getNodesTable() {
        return nodesTable;
    }
}
