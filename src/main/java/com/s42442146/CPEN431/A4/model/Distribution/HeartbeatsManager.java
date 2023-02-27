package com.s42442146.CPEN431.A4.model.Distribution;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatsManager {
    // Key is node id
    private ConcurrentHashMap<Integer, Long> heartBeats;
    private final static HeartbeatsManager instance = new HeartbeatsManager();
    public NodesCircle nodesCircle = NodesCircle.getInstance();
    private HeartbeatsManager() {
        heartBeats = new ConcurrentHashMap<>();
        for (int i = 0; i < nodesCircle.getStartupNodesSize(); i++) {
            heartBeats.put(i, 0l);
        }
    }

    public static HeartbeatsManager getInstance() {
        return instance;
    }

    public ConcurrentHashMap<Integer, Long> getHeartBeats() {
        return heartBeats;
    }

    public void updateHeartbeats(List<Long> receivedHeartbeats) {
        for (int id = 0; id < receivedHeartbeats.size(); id++) {

        }
    }
}
