//package com.g3.CPEN431.A9.Model.Distribution;
//
//import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
//import com.g3.CPEN431.A9.Model.Command;
//import com.g3.CPEN431.A9.Model.KVServer;
//import com.g3.CPEN431.A9.Model.Store.KVStore;
//import com.g3.CPEN431.A9.Model.Store.Value;
//import com.google.common.hash.Hashing;
//import com.google.protobuf.ByteString;
//import org.checkerframework.checker.units.qual.A;
//
//import java.net.DatagramSocket;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class NodeStatusChecker implements Runnable {
//    private final NodesCircle nodesCircle = NodesCircle.getInstance();
//    private final HeartbeatsManager heartbeatsManager = HeartbeatsManager.getInstance();
//    KeyTransferManager keyTransferManager = KeyTransferManager.getInstance();
//
//    private final KVStore store = KVStore.getInstance();
//
//    public NodeStatusChecker() {}
//
//    @Override
//    public void run() {
//        ConcurrentHashMap<Integer, Node> deadNodes = nodesCircle.getDeadNodesList();
//        ConcurrentHashMap<Integer, Node> aliveNodes = nodesCircle.getAliveNodesList();
//        ConcurrentHashMap<Integer, Node> allNodes = nodesCircle.getAllNodesList();
//        ArrayList<Integer> recoveredNodeIds = new ArrayList<>();
//
////        if(KVServer.port == 12385) {
////            System.out.println("12385 my time, " + System.currentTimeMillis() + ", " + (System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(nodesCircle.getCurrentNode().getId())));
////        }
//
//        for (Node node : allNodes.values()) {
//            //if (node == nodesCircle.getCurrentNode()) continue;
//            if (!heartbeatsManager.isNodeAlive(node) && aliveNodes.contains(node)) {
//
//                //If my predecessor dead, I will take the primary postion
//                // I will need my predecessor's place on the ring, before remove it
////                if (nodesCircle.getMyPredessors().contains(node))
////                    takePrimaryPosition(node);
//
//                nodesCircle.removeNode(node);
//
//                long time = System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(node.getId());
//                long time2 = heartbeatsManager.getHeartBeats().get(node.getId());
//                System.out.println(KVServer.port + " remove node: " + node.getPort()  + ", last update time " + time2 + ", from now past " + time);
//
//            } else if (heartbeatsManager.isNodeAlive(node) && !aliveNodes.contains(node)) {
//
//                nodesCircle.rejoinNode(node);
////                recoveredNodeIds.add(node.getId());
//
//                long time = System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(node.getId());
//                long time2 = heartbeatsManager.getHeartBeats().get(node.getId());
//                System.out.println(KVServer.port + " Adding back node: " + node.getPort() + " num servers left: "
//                        + nodesCircle.getAliveNodesCount() + ", last update time " + time2 + ", from now past " + time);
//            }
//        }
//
//        // Need the updated circle to update predecessor list
//        nodesCircle.updateMyPredecessor();
//
//        //  If my predecessor recovered, I will send keys to predecessor and two other replica (keys belong to my recovered successor)
//        // I will need my predecessor's place AFTER it is added to the ring
////        for(Integer nodeId: recoveredNodeIds) {
////            if (nodesCircle.getMyPredessors().containsKey(nodeId)) {
////                recoverPrimaryPosition(allNodes.get(nodeId));
////            }
////        }
//
////
////         If notify the recovered nodes for transfer
////        for (Node node: recoveredNodes) {
////            Set<Node> successorNodes = nodesCircle.findSuccessorNodes(node);
////            keyTransferManager.sendMessageToSuccessor(successorNodes, node);
////        }
//    }
//
//
//
//}
