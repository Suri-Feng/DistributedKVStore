package com.g3.CPEN431.A12.Model.Distribution;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatsManager {
    // Key is node id
    private final ConcurrentHashMap<Integer, Long> heartBeats;
    private final static HeartbeatsManager instance = new HeartbeatsManager();
    public NodesCircle nodesCircle = NodesCircle.getInstance();
    private final long metric;
    public final long mostPastTime = 50;


    private HeartbeatsManager() {
        heartBeats = new ConcurrentHashMap<>();
        for (int i = 0; i < nodesCircle.getStartupNodesSize(); i++) {
            heartBeats.put(i, nodesCircle.getCurrentNode().getId() == i? System.currentTimeMillis():0L);
        }
        // To find node recover -> a lower bound threshold
        // To find node suspended faster (and start key transfers faster, stage 1) -> set lower metric
        metric = (long) (50 * (Math.log(nodesCircle.getStartupNodesSize()) / Math.log(2) + 100));
    }

    public void updateHeartbeats(List<Long> receivedHeartbeats) {
        for (int id = 0; id < receivedHeartbeats.size(); id++) {
            heartBeats.put(id, Math.max(receivedHeartbeats.get(id), heartBeats.get(id)));
        }
    }

    public boolean isNodeAlive(Node node) {
        return (System.currentTimeMillis() - heartBeats.get(node.getId())) <= metric;
    }

    public static HeartbeatsManager getInstance() {
        return instance;
    }

    public ConcurrentHashMap<Integer, Long> getHeartBeats() {
        return heartBeats;
    }
}
