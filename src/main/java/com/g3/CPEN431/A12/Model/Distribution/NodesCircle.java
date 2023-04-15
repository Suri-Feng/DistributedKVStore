package com.g3.CPEN431.A12.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

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
    private final ConcurrentSkipListMap<Integer, ConcurrentHashMap<Integer, Node>> myPredecessors;
    private final ConcurrentSkipListMap<Integer, ConcurrentHashMap<Integer, Node>> mySuccessors;
    private int startupNodesSize;
    private int thisNodeId;

    private Node currentNode;
    private NodesCircle() {
        circle = new ConcurrentSkipListMap<>();
        aliveNodesList = new ConcurrentHashMap<>();
        allNodesList = new ConcurrentHashMap<>();
        deadNodesList = new ConcurrentHashMap<>();
        myPredecessors =  new ConcurrentSkipListMap<>();
        mySuccessors = new ConcurrentSkipListMap<>();
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
//            for (Map.Entry<Integer, Node> entry: circle.entrySet()) {
////            System.out.println(entry.getKey());
//                System.out.println(entry.getValue().getPort());
//            }
//        System.out.println("==========");
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



    public KeyValueRequest.HashRange getHashRangeByHash(int VN, Node PN) {
        Integer lowerKey = VN;
        Node node;
        do {
            lowerKey = circle.lowerKey(lowerKey);
            lowerKey = lowerKey == null ? circle.lastKey() : lowerKey;
            node = circle.get(lowerKey);
        } while (node == PN);

        return
                KeyValueRequest.HashRange.newBuilder()
                        .setMinRange(lowerKey + 1)
                        .setMaxRange(VN)
                        .build();
    }


    // Get current hash range
    public List<KeyValueRequest.HashRange> getCurrentKeyRangeOnNodeCircle(Node recoveredNode) {
        int hash1 = getCircleBucketFromHash(recoveredNode.getSha256Hash());
        int hash2 = getCircleBucketFromHash(recoveredNode.getSha512Hash());
        int hash3 = getCircleBucketFromHash(recoveredNode.getSha384Hash());
        int[] hashes = {hash1, hash2, hash3};

        List<KeyValueRequest.HashRange> hashRangeList = new ArrayList<>();

        for (int hash: hashes) {
            Integer lowerKey = hash;
            Node node;
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

    public ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Node>> updateMySuccessor() {
//        this.mySuccessors = findSuccessorNodesHashMap(this.getCurrentNode());
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Node>> newSuccForVNs = new ConcurrentHashMap<>();

        int vn1 = getCircleBucketFromHash(currentNode.getSha256Hash());
        int vn2 = getCircleBucketFromHash(currentNode.getSha512Hash());
        int vn3 = getCircleBucketFromHash(currentNode.getSha384Hash());
        int[] vns = {vn1, vn2, vn3};
        for (int vn: vns) {
            ConcurrentHashMap<Integer, Node> vnSucc = findThreeImmediateSuccessorsHashMap(vn);
            ConcurrentHashMap<Integer, Node> newSucc = new ConcurrentHashMap<>();
            for(Node node: vnSucc.values()) {
                if(this.mySuccessors.get(vn) == null || !this.mySuccessors.get(vn).contains(node))
                    newSucc.put(node.getId(), node);
            }
            if(newSucc.size() >= 1) {
                this.mySuccessors.put(vn, vnSucc);
                newSuccForVNs.put(vn, newSucc);
            }
        }

        return newSuccForVNs;
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

    public Node getNodeById(int id) {
        return allNodesList.get(id);
    }

    public ConcurrentHashMap<Integer, Node> getAliveNodesList() {
        return aliveNodesList;
    }

    public ConcurrentHashMap<Integer, Node> getAllNodesList() {
        return allNodesList;
    }

    public ConcurrentSkipListMap<Integer, ConcurrentHashMap<Integer, Node>> getMyPredessors() {
        return this.myPredecessors;
    }

    public ConcurrentSkipListMap<Integer, ConcurrentHashMap<Integer, Node>> getMySuccessors() {
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

    public void updateMyPredecessor() {

        int vn1 = getCircleBucketFromHash(currentNode.getSha256Hash());
        int vn2 = getCircleBucketFromHash(currentNode.getSha512Hash());
        int vn3 = getCircleBucketFromHash(currentNode.getSha384Hash());
        int[] vns = {vn1, vn2, vn3};

        for (int vn: vns) {
            ConcurrentHashMap<Integer, Node> vnPred = findOneImmediatePredecessorsHashMap(vn);
            ConcurrentHashMap<Integer, Node> newPred = new ConcurrentHashMap<>();
            for(Node node: vnPred.values()) {
                if(this.myPredecessors.get(vn) == null || !this.myPredecessors.get(vn).contains(node)) {
                    newPred.put(node.getId(), node);
//                    System.out.println(KVServer.port + "has pred" + node.getPort());
                }
            }
            if(newPred.size() >= 1) {
                this.myPredecessors.put(vn, vnPred);
            }
        }
    }

    public ConcurrentHashMap<Integer, Node> findOneImmediatePredecessorsHashMap(int hash) {
        Set<Node> nodes = findOneImmediatePredecessors(hash);
        ConcurrentHashMap<Integer, Node> nodesHashMap = new ConcurrentHashMap<>();
        for(Node node: nodes) {
            nodesHashMap.put(node.getId(), node);
        }
        return nodesHashMap;
    }

    // TODO: CHANGE TO FIND ONE
    public Set<Node> findOneImmediatePredecessors(int ringKey) {
        Set<Node> nodes = new HashSet<>();
        ConcurrentNavigableMap<Integer, Node> headMap = circle.headMap(ringKey);

        Iterator<Map.Entry<Integer, Node>> iterator;
        if (headMap.isEmpty()) {
            iterator = circle.tailMap(ringKey).descendingMap().entrySet().iterator();

            int nodesFound = 0;
            while (nodesFound < 1 && iterator.hasNext()) {
                Node node = iterator.next().getValue();
                if (node != currentNode && nodes.add(node)) {
                    nodesFound++;
                }
            }
            return nodes;
        }

        iterator = headMap.descendingMap().entrySet().iterator();
        int nodesFound = 0;
        boolean updated = false;
        if (!iterator.hasNext()) {
            iterator = circle.tailMap(ringKey).descendingMap().entrySet().iterator();
            updated = true;
        }
        while (nodesFound < 3 && iterator.hasNext()) {
            Node node = iterator.next().getValue();
            if (node != currentNode && nodes.add(node)) {
                nodesFound++;
            }

            if (!updated && !iterator.hasNext()) {
                iterator = circle.tailMap(ringKey).descendingMap().entrySet().iterator();
                updated = true;
            }
        }
        return nodes;
    }

    public ConcurrentHashMap<Integer, Node> findThreeImmediateSuccessorsHashMap(int hash) {
        Set<Node> nodes = findThreeImmediateSuccessors(hash);
        ConcurrentHashMap<Integer, Node> nodesHashMap = new ConcurrentHashMap<>();
        for(Node node: nodes) {
            nodesHashMap.put(node.getId(), node);
        }
        return nodesHashMap;
    }

    public Set<Node> findThreeImmediateSuccessors(int ringKey) {
        //int ringKey = getCircleBucketFromHash(hash);
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
        boolean updated = false;
        if (!iterator.hasNext()) {
            iterator = circle.headMap(ringKey).entrySet().iterator();
            updated = true;
        }
        while (nodesFound < 3 && iterator.hasNext()) {
            Node node = iterator.next().getValue();
            // can't be the same as primary, can't be the same as each other
            if (node != primary && nodes.add(node)) {
                nodesFound++;
            }

            if (!updated && !iterator.hasNext()) {
                iterator = circle.headMap(ringKey).entrySet().iterator();
                updated = true;
            }
        }
        return nodes;
    }


    // TODO
    public int findSuccVNbyRingHash(int ringHash) {
        ConcurrentNavigableMap<Integer, ConcurrentHashMap<Integer, Node>> tailMap = mySuccessors.tailMap(ringHash);
        return tailMap.isEmpty()? mySuccessors.firstKey(): tailMap.firstKey();
    }
}
