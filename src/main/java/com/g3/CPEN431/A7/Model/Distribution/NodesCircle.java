package com.g3.CPEN431.A7.Model.Distribution;

import java.util.ArrayList;
import java.util.Map;
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
    private NodesCircle() {
        circle = new ConcurrentSkipListMap<>();
        aliveNodesList = new ConcurrentHashMap<>();
        allNodesList = new ConcurrentHashMap<>();
        deadNodesList = new ConcurrentHashMap<>();
        thisNodeId = -1;
    }
    public void buildHashCircle() {
        for (Node node: allNodesList.values()) {
            int hash1 = getCircleBucketFromHash(node.getSha256Hash());
            int hash2 = getCircleBucketFromHash(node.getSha512Hash());
            int hash3 = getCircleBucketFromHash(node.getSha384Hash());

            circle.put(hash1, node);
            circle.put(hash2, node);
            circle.put(hash3, node);
        }
    }

    public void setNodeList(ArrayList<Node> list) {
        for (Node node: list) {
            this.aliveNodesList.put(node.getId(), node);
            this.allNodesList.put(node.getId(), node);

        }
        this.startupNodesSize = this.allNodesList.size();
    }

    public int findRingKeyByHash(int hash) {
        int keyHash = getCircleBucketFromHash(hash);
        ConcurrentNavigableMap<Integer, Node> tailMap = circle.tailMap(keyHash);
        return tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
    }

    public Node findNextNode(int ringHash) {
        Map.Entry<Integer, Node> higherEntry = circle.higherEntry(ringHash);
        return higherEntry != null ? higherEntry.getValue() : circle.firstEntry().getValue();
    }

    /**
     * Gets hash on circle corresponding to a node hash
     * @param hash hash of a node
     */
    public int getCircleBucketFromHash(int hash) {
        int n = Integer.MAX_VALUE;
        return  hash % n < 0 ? hash % n + n : hash % n;
    }
    public void removeNode(int ringKey) {
        Node node = circle.get(ringKey);
        aliveNodesList.remove(node.getId());
        circle.remove(ringKey);
        deadNodesList.put(node.getId(), node);
    }

    public void rejoinNode(Node node) {
        aliveNodesList.put(node.getId(), node);
        int hash1 = getCircleBucketFromHash(node.getSha256Hash());
        int hash2 = getCircleBucketFromHash(node.getSha512Hash());
        int hash3 = getCircleBucketFromHash(node.getSha384Hash());

        circle.put(hash1, node);
        circle.put(hash2, node);
        circle.put(hash3, node);
        deadNodesList.remove(node.getId());
    }

    /**
     * Gets Node corresponding to an address and port
     */
    public Node getNodeFromIp(String address, int port) {
        for (Node node: allNodesList.values()) {
            if (node.getAddress().getHostAddress().equals(address) && node.getPort() == port) {
                return node;
            }
        }
        return null;
    }

    public void setThisNodeId(int id) {
        this.thisNodeId = id;
    }

    public int getThisNodeId() {
        return thisNodeId;
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
