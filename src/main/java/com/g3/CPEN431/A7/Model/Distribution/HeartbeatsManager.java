package com.g3.CPEN431.A7.Model.Distribution;

import com.g3.CPEN431.A7.Model.KVServer;

import java.util.ArrayList;
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
        metric = (long) (10 * (Math.log(nodesCircle.getStartupNodesSize()) / Math.log(2) + 100));
    }

    public void updateHeartbeats(List<Long> receivedHeartbeats) {
        int myId = nodesCircle.getThisNodeId();

        for (int id = 0; id < receivedHeartbeats.size(); id++) {
            if (id != myId) {
                heartBeats.put(id, Math.max(receivedHeartbeats.get(id), heartBeats.get(id)));
            }
        }
    }

    public boolean isNodeAlive(Node node) {
        if (node.getId() == nodesCircle.getThisNodeId()) {
            return true;
        }
        return System.currentTimeMillis() - heartBeats.get(node.getId()) <= metric;
    }

    public List<Node> recoverLiveNodes() {
        List<Node> recoveredNodes = new ArrayList<>();
        ConcurrentHashMap<Integer, Node> deadNodes = nodesCircle.getDeadNodesList();
        ConcurrentHashMap<Integer, Node> aliveNodes = nodesCircle.getAliveNodesList();
        for (Map.Entry<Integer, Node> nodeList: deadNodes.entrySet()) {
            Node node = nodeList.getValue();
            if (isNodeAlive(node) && !aliveNodes.containsKey(node.getId())) {
                nodesCircle.rejoinNode(node);
                recoveredNodes.add(node);
                System.out.println(KVServer.port + " Adding back node: " + node.getPort() + " num servers left: "
                        + nodesCircle.getAliveNodesCount());
            }
        }
        return recoveredNodes;
    }

    public static HeartbeatsManager getInstance() {
        return instance;
    }

    public ConcurrentHashMap<Integer, Long> getHeartBeats() {
        return heartBeats;
    }
}
