package com.g3.CPEN431.A7.Model.Distribution;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class NodesCircle {
    private final static NodesCircle instance = new NodesCircle();
    private final ConcurrentSkipListMap<Integer, Node> circle;
    public static NodesCircle getInstance() {
        return instance;
    }
    private final ConcurrentHashMap<Integer, Node> aliveNodesList;
    private final ConcurrentHashMap<Integer, Node> allNodesList;
    private final ConcurrentHashMap<Integer, Node> deadNodesList;
    private int startupNodesSize;
    private int thisNodeId;
    private int thisNodeRingHash;
    private NodesCircle() {
        circle = new ConcurrentSkipListMap<>();
        aliveNodesList = new ConcurrentHashMap<>();
        allNodesList = new ConcurrentHashMap<>();
        deadNodesList = new ConcurrentHashMap<>();
        thisNodeRingHash = -1;
        thisNodeId = -1;
    }
    public void buildHashCircle() {
        for (Node node: aliveNodesList.values()) {
            int hash = getCircleHashFromNodeHash(node.getHash());
            circle.put(hash, node);
        }
    }

    public void setThisNodeId(int id) {
        this.thisNodeId = id;
    }

    public void setThisNodeRingHash(int hash) {
        this.thisNodeRingHash = hash;
    }

    public int getThisNodeId() {
        return thisNodeId;
    }

    public int getThisNodeRingHash() {
        return thisNodeRingHash;
    }

    public void setNodeList(ArrayList<Node> list) {
        for (Node node: list) {
            this.aliveNodesList.put(node.getId(), node);
            this.allNodesList.put(node.getId(), node);

        }
        this.startupNodesSize = this.aliveNodesList.size();
    }

    public int getStartupNodesSize() {
        return this.startupNodesSize;
    }

    public int getAliveNodesCount() {
        return this.aliveNodesList.size();
    }

    public Node getNodeById(int id) {
        return allNodesList.get(id);
    }

    public ConcurrentSkipListMap<Integer, Node> getCircle() {
        return circle;
    }

    public int findRingKeyByHash(int nodeHash) {
        //int n = 1 << (aliveNodesList.size());
        // int keyHash = nodeHash % n < 0 ? nodeHash % n + n : nodeHash % n;
        int n = startupNodesSize*100;
        int keyHash = nodeHash % n < 0 ? (-nodeHash) % n : nodeHash % n;

        ConcurrentNavigableMap<Integer, Node> tailMap = circle.tailMap(keyHash);
        return tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
    }

    /**
     * Gets hash on circle corresponding to a node hash
     * @param hash hash of a node
     */
    public int getCircleHashFromNodeHash(long hash) {
//        int numNodes = aliveNodesList.size();
//        int n = 1 << numNodes;
//        return  (int) (hash % n < 0 ? hash % n + n : hash % n);
        int n = startupNodesSize*100;
        return (int) (hash % n < 0 ? (-hash) % n : hash % n);
    }
    public void removeNode(int ringKey) {
        Node node = circle.get(ringKey);
        aliveNodesList.remove(node.getId());
        circle.remove(ringKey);
        deadNodesList.put(node.getId(), node);
    }

    public void rejoinNode(Node node) {
        aliveNodesList.put(node.getId(), node);
        int hash = getCircleHashFromNodeHash(node.getHash());
        circle.put(hash, node);
        deadNodesList.remove(node.getId());
    }

    /**
     * Gets Node corresponding to an ip
     * @param ip ip of a node in the format of address + port
     */
    public Node getNodeFromIp(String ip) {
        long id = Node.hashTo64bit(ip);
        return getCircle().get(getCircleHashFromNodeHash(id));
    }

    public ConcurrentHashMap<Integer, Node> getAliveNodesList() {
        return aliveNodesList;
    }

    public ConcurrentHashMap<Integer, Node> getAllNodesList() {
        return allNodesList;
    }
    public ConcurrentHashMap<Integer, Node> getDeadNodesList() {
        return deadNodesList;
    }
}
