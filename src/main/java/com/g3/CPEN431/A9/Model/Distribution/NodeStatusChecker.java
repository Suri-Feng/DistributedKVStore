package com.g3.CPEN431.A9.Model.Distribution;

import com.g3.CPEN431.A9.Model.KVServer;

import java.util.concurrent.ConcurrentHashMap;

public class NodeStatusChecker implements Runnable {
    private final NodesCircle nodesCircle = NodesCircle.getInstance();
    private final HeartbeatsManager heartbeatsManager = HeartbeatsManager.getInstance();


    @Override
    public void run() {
        ConcurrentHashMap<Integer, Node> deadNodes = nodesCircle.getDeadNodesList();
        ConcurrentHashMap<Integer, Node> aliveNodes = nodesCircle.getAliveNodesList();
        ConcurrentHashMap<Integer, Node> allNodes = nodesCircle.getAllNodesList();
        for (Node node : allNodes.values()) {
            if (node != nodesCircle.getCurrentNode()) continue;
            if (!heartbeatsManager.isNodeAlive(node) && nodesCircle.getAliveNodesList().contains(node))
            {
                nodesCircle.removeNode(node);
                System.out.println(KVServer.port + " remove node: " + node.getPort());
            }
            else if (heartbeatsManager.isNodeAlive(node) && !aliveNodes.containsKey(node.getId()))
            {
                nodesCircle.rejoinNode(node);
                System.out.println(KVServer.port + " Adding back node: " + node.getPort() + " num servers left: "
                        + nodesCircle.getAliveNodesCount());


            }

//                    boolean predecessorUpdated = false;
//        for (Map.Entry<Integer, Node> entry: nodesCircle.getDeadNodesList().entrySet()) {
//            if(nodesCircle.getMyPredessors().containsKey(entry.getKey())) {
//                predecessorUpdated = true;
//                replication.takePrimaryPosition(entry.getValue());
//                nodesCircle.getMyPredessors().remove(entry.getKey());
//            }
//        }
//
//        if(predecessorUpdated) nodesCircle.updateMyPredecessor();
//
//        for (Node node: recoveredNodes) {
//            Set<Node> successorNodes = nodesCircle.findSuccessorNodes(node);
//            keyTransferManager.sendMessageToSuccessor(successorNodes, node);
//        }
        }
    }
}
