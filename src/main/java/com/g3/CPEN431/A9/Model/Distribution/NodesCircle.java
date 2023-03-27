package com.g3.CPEN431.A9.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.Internal;
import org.checkerframework.checker.units.qual.C;

import java.util.*;
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
    private ConcurrentHashMap<Integer, Node> myPredecessors;
    private ConcurrentHashMap<Integer, Node> mySuccessors;
    private int startupNodesSize;
    private int thisNodeId;

    private Node currentNode;
    private NodesCircle() {
        circle = new ConcurrentSkipListMap<>();
        aliveNodesList = new ConcurrentHashMap<>();
        allNodesList = new ConcurrentHashMap<>();
        deadNodesList = new ConcurrentHashMap<>();
        myPredecessors = new ConcurrentHashMap<>();
        thisNodeId = -1;
        currentNode = null;
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
//        System.out.println("==========");
//        for (Map.Entry<Integer, Node> entry: circle.entrySet()) {
////            System.out.println(entry.getKey());
//            System.out.println(entry.getValue().getPort());
//        }
//        System.out.println("==========")
    }

    public void setNodeList(ArrayList<Node> list) {
        for (Node node: list) {
            this.allNodesList.put(node.getId(), node);
        }
        this.startupNodesSize = this.allNodesList.size();
    }

    public Node findCorrectNodeByHash(int hash) {
        int ringKey = getCircleBucketFromHash(hash);
        ConcurrentNavigableMap<Integer, Node> tailMap = circle.tailMap(ringKey);
        return tailMap.isEmpty() ? circle.firstEntry().getValue() : tailMap.firstEntry().getValue();
    }

    public int getCircleBucketFromHash(int hash) {
        int n = Integer.MAX_VALUE;
        return  hash % n < 0 ? hash % n + n : hash % n;
    }
    public void removeNode(Node node) {
        circle.remove(getCircleBucketFromHash(node.getSha256Hash()));
        circle.remove(getCircleBucketFromHash(node.getSha384Hash()));
        circle.remove(getCircleBucketFromHash(node.getSha512Hash()));
        aliveNodesList.remove(node.getId());
        deadNodesList.put(node.getId(), node);
    }

    public void rejoinNode(Node node) {
        int hash1 = getCircleBucketFromHash(node.getSha256Hash());
        int hash2 = getCircleBucketFromHash(node.getSha512Hash());
        int hash3 = getCircleBucketFromHash(node.getSha384Hash());
        circle.put(hash1, node);
        circle.put(hash2, node);
        circle.put(hash3, node);
        aliveNodesList.put(node.getId(), node);
        deadNodesList.remove(node.getId());
    }


//    public List<KeyValueRequest.HashRange> getInitialNodeRange(Node node) {
//
//    }

    // Get current hash range
    public List<KeyValueRequest.HashRange> getRecoveredNodeRange(Node recoveredNode) {
        int hash1 = getCircleBucketFromHash(recoveredNode.getSha256Hash());
        int hash2 = getCircleBucketFromHash(recoveredNode.getSha512Hash());
        int hash3 = getCircleBucketFromHash(recoveredNode.getSha384Hash());
        int[] hashes = {hash1, hash2, hash3};

        List<KeyValueRequest.HashRange> hashRangeList = new ArrayList<>();

        for (int hash: hashes) {
            Integer lowerKey = hash;
            Node node = null;
            do {
                lowerKey = circle.lowerKey(lowerKey);
                lowerKey = lowerKey == null ? circle.lastKey() : lowerKey;
                node = circle.get(lowerKey);
            } while (node == recoveredNode);

            hashRangeList.add(
                    KeyValueRequest.HashRange.newBuilder()
                            .setMinRange(lowerKey + 1)
                            .setMaxRange(hash)
                            .build());
        }
        return hashRangeList;
    }

    public void updateMySuccessor() {
        this.mySuccessors = findSuccessorNodesHashMap(this.getCurrentNode());
    }

    public ConcurrentHashMap<Integer, Node> findSuccessorNodesHashMap(Node recoveredNode) {
//        if (aliveNodesList.size() <= 1) return null;
        int hash1 = getCircleBucketFromHash(recoveredNode.getSha256Hash());
        int hash2 = getCircleBucketFromHash(recoveredNode.getSha512Hash());
        int hash3 = getCircleBucketFromHash(recoveredNode.getSha384Hash());
        int[] hashes = {hash1, hash2, hash3};
        ConcurrentHashMap<Integer, Node> nodes = new ConcurrentHashMap<>();

        for (int hash: hashes) {
            Node node = null;
            Integer higherKey = hash;
            do {
                higherKey = circle.higherKey(higherKey);
                higherKey = higherKey == null ? circle.firstKey() : higherKey;
                node = circle.get(higherKey);
            } while (node == recoveredNode);
            nodes.put(node.getId(), node);
        }
        return nodes;
    }

    public Set<Node> findSuccessorNodes(Node recoveredNode) {
//        if (aliveNodesList.size() <= 1) return null;
        int hash1 = getCircleBucketFromHash(recoveredNode.getSha256Hash());
        int hash2 = getCircleBucketFromHash(recoveredNode.getSha512Hash());
        int hash3 = getCircleBucketFromHash(recoveredNode.getSha384Hash());
        int[] hashes = {hash1, hash2, hash3};
        Set<Node> nodes = new HashSet<>();

        for (int hash: hashes) {
            Node node = null;
            Integer higherKey = hash;
            do {
                higherKey = circle.higherKey(higherKey);
                higherKey = higherKey == null ? circle.firstKey() : higherKey;
                node = circle.get(higherKey);
            } while (node == recoveredNode);
            nodes.add(node);
        }
        return nodes;
    }

    public void updateMyPredecessor() {
        this.myPredecessors = findPredessorNodes(this.getCurrentNode());
    }

    public ConcurrentHashMap<Integer, Node> findPredessorNodes(Node node) {
//        if(aliveNodesList.size() <= 1) return null;
        int hash1 = getCircleBucketFromHash(node.getSha256Hash());
        int hash2 = getCircleBucketFromHash(node.getSha512Hash());
        int hash3 = getCircleBucketFromHash(node.getSha384Hash());
        int[] hashes = {hash1, hash2, hash3};
        ConcurrentHashMap<Integer, Node> nodes = new ConcurrentHashMap<>();

        for (int hash: hashes) {
            Node predNode = null;
            Integer lowerKey = hash;
            do {
                lowerKey = circle.lowerKey(lowerKey);
                lowerKey = lowerKey == null ? circle.lastKey() : lowerKey;
                predNode = circle.get(lowerKey);
            } while (predNode == node);
            nodes.put(predNode.getId(), predNode);
        }
        return nodes;
    }

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
        currentNode = getNodeById(id);
        updateMyPredecessor();
        updateMySuccessor();
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

    public int getDeadNodesCount() {
        return this.deadNodesList.size();
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

    public ConcurrentHashMap<Integer, Node> getMyPredessors() {
        return this.myPredecessors;
    }

    public ConcurrentHashMap<Integer, Node> getMySuccessors() {
        return this.mySuccessors;
    }

    public Node getCurrentNode() {
        return currentNode;
    }

    public Node findNodebyKey(ByteString key) {
        String sha256 = Hashing.sha256()
                .hashBytes(key.toByteArray()).toString();

        return findCorrectNodeByHash(sha256.hashCode());
    }

    public ConcurrentHashMap<Integer, Node> findThreeImmediateSuccessorsHashMap(int hash) {
        Set<Node> nodes = findThreeImmediateSuccessors(hash);
        ConcurrentHashMap<Integer, Node> nodesHashMap = new ConcurrentHashMap<>();
        for(Node node: nodes) {
            nodesHashMap.put(node.getId(), node);
        }
        return nodesHashMap;
    }

    public Set<Node> findThreeImmediateSuccessors(int hash) {
        int ringKey = getCircleBucketFromHash(hash);
        ConcurrentHashMap<Integer, Node> succNodes = new ConcurrentHashMap<>();
        Set<Node> nodes = new HashSet<>();
        ConcurrentNavigableMap<Integer, Node> tailMap = circle.tailMap(ringKey);

        Iterator<Map.Entry<Integer, Node>> iterator;
        if (tailMap.isEmpty()) {
            iterator = circle.entrySet().iterator();
            Node primary = iterator.next().getValue(); // primary node

            int nodesFound = 0;
            while (nodesFound < 3 && iterator.hasNext()) {
                Node node = iterator.next().getValue();
                // can't be the same as primary, can't be the same as each other
                if (node != primary && nodes.add(node)) {
                    nodesFound++;
                }
            }
            return nodes;
        }

        iterator = tailMap.entrySet().iterator();
        Node primary = iterator.next().getValue();
        int nodesFound = 0;
        while (nodesFound < 3) {
            if (iterator.hasNext()) {
                Node node = iterator.next().getValue();
                // can't be the same as primary, can't be the same as each other
                if (node != primary && nodes.add(node)) {
                    nodesFound++;
                }
            } else {
                iterator = circle.entrySet().iterator();
            }
        }
        return nodes;
    }
}
