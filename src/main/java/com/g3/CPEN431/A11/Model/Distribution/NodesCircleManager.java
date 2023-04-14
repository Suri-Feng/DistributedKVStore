package com.g3.CPEN431.A11.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NodesCircleManager {
    private final static NodesCircleManager instance = new NodesCircleManager();

    public static NodesCircleManager getInstance() {
        return instance;
    }

    KeyTransferManager keyTransferManager = KeyTransferManager.getInstance();
    HeartbeatsManager heartbeatsManager = HeartbeatsManager.getInstance();
    NodesCircle nodesCircle = NodesCircle.getInstance();

    public void updateNodeCircle() {
        ConcurrentHashMap<Integer, Node> aliveNodes = nodesCircle.getAliveNodesList();
        ConcurrentHashMap<Integer, Node> allNodes = nodesCircle.getAllNodesList();
        ConcurrentHashMap<Node, List<KeyValueRequest.HashRange>> removedPrimaryHashRanges = new ConcurrentHashMap<>();
        ConcurrentHashMap<Node, List<KeyValueRequest.HashRange>> recoveredPrimaryHashRanges = new ConcurrentHashMap<>();

        for (Node node : allNodes.values()) {
            if (!heartbeatsManager.isNodeAlive(node) && aliveNodes.contains(node)) {

                // If my predecessor dead, I will take the primary postion
                // I will need my predecessor's place on the ring, before remove it
                for (ConcurrentHashMap<Integer, Node> prePreds : nodesCircle.getMyPredessors().values()) {
                    if (prePreds.contains(node))
                        removedPrimaryHashRanges.put(node, nodesCircle.getRecoveredNodeRange(node));
                }

                nodesCircle.removeNode(node);
            } else if (heartbeatsManager.isNodeAlive(node) && !aliveNodes.contains(node)) {

                nodesCircle.rejoinNode(node);
                boolean contains = false;
                for (ConcurrentHashMap<Integer, Node> prePreds : nodesCircle.getMyPredessors().values()) {
                    if (prePreds.contains(node)) contains = true;
                }
                if (!contains)
                    recoveredPrimaryHashRanges.put(node, nodesCircle.getRecoveredNodeRange(node));
            }
        }

        // If my successor changed (died or recovered), I will replace the backup
        // I will need new successor's place after ring updated
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Node>> newSuccsForVNs = nodesCircle.updateMySuccessor();
        for (Integer VN : newSuccsForVNs.keySet()) {
            for (Node newBackup : newSuccsForVNs.get(VN).values()) {
                keyTransferManager.updateBackupPosition(newBackup, VN);
            }
        }


        for (Node node : removedPrimaryHashRanges.keySet())
            keyTransferManager.takePrimaryPosition(removedPrimaryHashRanges.get(node));

        //  If my predecessor recovered, I will send keys to predecessor and two other replica (keys belong to my recovered successor)
        // I will need my predecessor's place AFTER it is added to the ring
        for (Node node : recoveredPrimaryHashRanges.keySet()) {
            keyTransferManager.recoverPrimaryPosition(node, recoveredPrimaryHashRanges.get(node));
        }
        nodesCircle.updateMyPredecessor();
//
    }
}
