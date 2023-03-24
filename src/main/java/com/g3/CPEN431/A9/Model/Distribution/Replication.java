package com.g3.CPEN431.A9.Model.Distribution;

import com.google.protobuf.ByteString;

public class Replication {
    public boolean isPrimary(ByteString key) {
        NodesCircle nodesCircle = NodesCircle.getInstance();
        return nodesCircle.findNodebyKey(key).getId() == nodesCircle.getThisNodeId();
    }

    // Forward writes (to primary)
    // Propogate writes (from primary)
    // Reply writes (to primary)

    // Complete write
}
