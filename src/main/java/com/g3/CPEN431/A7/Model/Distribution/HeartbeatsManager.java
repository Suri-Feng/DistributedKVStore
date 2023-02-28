package com.g3.CPEN431.A7.Model.Distribution;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatsManager {
    // Key is node id
    private final ConcurrentHashMap<Integer, Long> heartBeats;
    private final static HeartbeatsManager instance = new HeartbeatsManager();
    public NodesCircle nodesCircle = NodesCircle.getInstance();
    private final long metric;

    private HeartbeatsManager() {
        heartBeats = new ConcurrentHashMap<>();
        for (int i = 0; i < nodesCircle.getStartupNodesSize(); i++) {
            heartBeats.put(i, 0L);
        }
        metric = (long) (10 * (Math.log(nodesCircle.getStartupNodesSize()) / Math.log(2) + 60));
    }

    public static HeartbeatsManager getInstance() {
        return instance;
    }

    public ConcurrentHashMap<Integer, Long> getHeartBeats() {
        return heartBeats;
    }

    public void updateHeartbeats(List<Long> receivedHeartbeats) {
        int myId = nodesCircle.getThisNodeId();

        for (int id = 0; id < receivedHeartbeats.size(); id++) {
            if (id != myId) {
                heartBeats.put(id, Math.max(receivedHeartbeats.get(id), heartBeats.get(id)));
            }
//            System.out.println("node " + id + " is alive: " +  isNodeAlive(id));
        }
    }

    public boolean isNodeAlive(int id) {
        if (id == nodesCircle.getThisNodeId()) {
            return true;
        }
        return System.currentTimeMillis() - heartBeats.get(id) <= metric;
    }

    public void recoverLiveNodes() {
        ConcurrentHashMap<Integer, Node> deadNodes = nodesCircle.getDeadNodesList();
        ConcurrentHashMap<Integer, Node> aliveNodes = nodesCircle.getAliveNodesList();
        for (Map.Entry<Integer, Node> nodeList: deadNodes.entrySet()) {
            Node node = nodeList.getValue();
            if (isNodeAlive(node.getId()) && !aliveNodes.containsKey(node.getId())) {
                nodesCircle.rejoinNode(node);
                System.out.println("Adding back node: " + node.getPort() + " num servers left: "
                        + nodesCircle.getAliveNodesCount());
            }
        }
    }
}
