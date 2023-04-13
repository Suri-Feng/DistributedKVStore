package com.g3.CPEN431.A9.Model.Distribution;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
public class NodeCircleManager {
    private final static NodeCircleManager instance = new NodeCircleManager();

    public static NodeCircleManager getInstance() {
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
//        ConcurrentHashMap<Integer, Node> recoveredNodes = new ConcurrentHashMap<>();
//        ConcurrentSkipListMap<Integer, ConcurrentHashMap<Integer, Node>> successorNodes = nodesCircle.getMySuccessors();

//        if(KVServer.port == 12385) {
//            System.out.println("12385 my time, " + System.currentTimeMillis() + ", " + (System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(nodesCircle.getCurrentNode().getId())));
//        }

        for (Node node : allNodes.values()) {
            //if (node == nodesCircle.getCurrentNode()) continue;
            if (!heartbeatsManager.isNodeAlive(node) && aliveNodes.contains(node)) {

                //If my predecessor dead, I will take the primary postion
                // I will need my predecessor's place on the ring, before remove it
                // TODO: Optimize this
                boolean contains = false;
                for (ConcurrentHashMap<Integer, Node> prePreds : nodesCircle.getMyPredessors().values()) {
                    if (prePreds.contains(node)) contains = true;
                }
                if (!contains)
                    removedPrimaryHashRanges.put(node, nodesCircle.getRecoveredNodeRange(node));

                nodesCircle.removeNode(node);

//                long time = System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(node.getId());
//                long time2 = heartbeatsManager.getHeartBeats().get(node.getId());
//                System.out.println(KVServer.port + " remove node: " + node.getPort()  + ", last update time " + time2 + ", from now past " + time);

            } else if (heartbeatsManager.isNodeAlive(node) && !aliveNodes.contains(node)) {

                nodesCircle.rejoinNode(node);
                // TODO: The same thing as removed primary -> to get range here, since range might have been changed when send msg NO
//                recoveredNodes.put(node.getId(), node);
                boolean contains = false;
                for (ConcurrentHashMap<Integer, Node> prePreds : nodesCircle.getMyPredessors().values()) {
                    if (prePreds.contains(node)) contains = true;
                }
                if (!contains)
                    recoveredPrimaryHashRanges.put(node, nodesCircle.getRecoveredNodeRange(node));

//                long time = System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(node.getId());
//                long time2 = heartbeatsManager.getHeartBeats().get(node.getId());
//                System.out.println(KVServer.port + " Adding back node: " + node.getPort() + " num servers left: "
//                        + nodesCircle.getAliveNodesCount() + ", last update time " + time2 + ", from now past " + time);
            }
        }

        // If my successor changed (died or recovered), I will replace the backup
        // I will need new successor's place after ring updated
        //TODO: Here might be concurrent issue
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Node>> newSuccsForVNs = nodesCircle.updateMySuccessor();
        for (Integer VN : newSuccsForVNs.keySet()) {
            for (Node newBackup : newSuccsForVNs.get(VN).values()) {
                keyTransferManager.updateBackupPosition(newBackup, VN);
            }
        }


        for (Node node : removedPrimaryHashRanges.keySet())
            keyTransferManager.takePrimaryPosition(removedPrimaryHashRanges.get(node));

        // Need the updated circle to update predecessor list
//        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Node>> newPredsForVNs = nodesCircle.updateMyPredecessor();
//        for(Integer VN: newPredsForVNs.keySet()) {
//            for (Node newPrimary : newPredsForVNs.get(VN).values()) {
//                recoverPrimaryPosition(newPrimary, VN);
//            }
//        }

        //  If my predecessor recovered, I will send keys to predecessor and two other replica (keys belong to my recovered successor)
        // I will need my predecessor's place AFTER it is added to the ring
        for (Node node : recoveredPrimaryHashRanges.keySet()) {
            keyTransferManager.recoverPrimaryPosition(node, recoveredPrimaryHashRanges.get(node));
        }
//        for (Node node: newPreds.values()) {
//            recoverPrimaryPosition(node);
//        }
//        for(Node node: recoveredNodes.values()) {
//            if (nodesCircle.getMyPredessors().contains(node)) {
//                recoverPrimaryPosition(node);
//            }
//        }
        nodesCircle.updateMyPredecessor();
//
    }
}
