package com.s42442146.CPEN431.A4.model.Distribution;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class NodesCircle {
    private final static NodesCircle instance = new NodesCircle();
    private ConcurrentSkipListMap<Integer, Node> circle;
    public static NodesCircle getInstance() {
        return instance;
    }
    private ConcurrentHashMap<Integer, Node> nodesList;
    private int startupNodesSize;
    private int thisNodeId;
    private int thisNodeHash;
    private NodesCircle() {
        circle = new ConcurrentSkipListMap<>();
        nodesList = new ConcurrentHashMap<>();
        thisNodeHash = -1;
        thisNodeId = -1;
    }
    public void buildHashCircle() {
        for (Node node: nodesList.values()) {
            int hash = getCircleHashFromNodeHash(node.getHash());
            circle.put(hash, node);
        }
    }

    public void setThisNodeId(int id) {
        this.thisNodeId = id;
    }

    public void setThisNodeHash(int hash) {
        this.thisNodeHash = hash;
    }

    public int getThisNodeId() {
        return thisNodeId;
    }

    public int getThisNodeHash() {
        return thisNodeHash;
    }

    public void setNodeList(ArrayList<Node> list) {
        for (Node node: list) {
            this.nodesList.put(node.getId(), node);
        }
        this.startupNodesSize = this.nodesList.size();
    }

    public int getStartupNodesSize() {
        return startupNodesSize;
    }

    public Node getNodeById(int id) {
        return nodesList.get(id);
    }

    public ConcurrentSkipListMap<Integer, Node> getCircle() {
        return circle;
    }

    public int findRingKeyByHash(int key) {
        int n = 1 << (nodesList.size());
        int keyHash = key % n < 0 ? key % n + n : key % n;

        ConcurrentNavigableMap<Integer, Node> tailMap = circle.tailMap(keyHash);
        return tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
    }

    /**
     * Gets hash on circle corresponding to a node hash
     * @param hash hash of a node
     * @return
     */
    public int getCircleHashFromNodeHash(long hash) {
        int numNodes = nodesList.size();
        int n = 1 << numNodes;
        return  (int) (hash % n < 0 ? hash % n + n : hash % n);
    }
    public void removeNode(int key) {
        Node node = circle.get(key);
        nodesList.remove(node.getId());
        circle.remove(key);
    }

    /**
     * Gets Node corresponding to an ip
     * @param ip ip of a node in the format of address + port
     * @return
     */
    public Node getNodeFromIp(String ip) {
        long id = Node.hashTo64bit(ip);
        return getCircle().get(getCircleHashFromNodeHash(id));
    }
}
