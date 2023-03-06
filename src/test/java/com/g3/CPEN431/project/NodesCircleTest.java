package com.g3.CPEN431.project;

import com.g3.CPEN431.A7.App;
import com.g3.CPEN431.A7.Model.Distribution.Node;
import com.g3.CPEN431.A7.Model.Distribution.NodesCircle;
import org.junit.*;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Optional;

import static org.junit.Assert.*;

public class NodesCircleTest {
    NodesCircle nodesCircle = NodesCircle.getInstance();

    public NodesCircleTest() throws IOException {
        nodesCircle.setNodeList(App.parseNodes());
        nodesCircle.buildHashCircle();
        Node node = nodesCircle.getNodeById(0);
        nodesCircle.setThisNodeId(node.getId());
    }

    @Test
    public void testCircleBuild() throws UnknownHostException {
        assertEquals(nodesCircle.getNodeById(0).getPort(), 12347);
        assertEquals(nodesCircle.getThisNodeId(), 0);
    }

    @Test
    public void testGetRingHashIfMyPredecessor() {
        assertNull(nodesCircle.getRingHashIfMyPredecessor(0));
        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(1)), Optional.of(357678050));
        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(9)), Optional.of(1411844308));
        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(6)), Optional.of(1907984985));
        nodesCircle.removeNode(nodesCircle.getNodeById(1));
        assertNull(nodesCircle.getRingHashIfMyPredecessor(1));
        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(12)), Optional.of(348429519));

        nodesCircle.removeNode(nodesCircle.getNodeById(12));
        assertNull(nodesCircle.getRingHashIfMyPredecessor(12));
        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(16)), Optional.of(323137670));

        nodesCircle.removeNode(nodesCircle.getNodeById(16));
        nodesCircle.removeNode(nodesCircle.getNodeById(14));
        nodesCircle.removeNode(nodesCircle.getNodeById(15));
        nodesCircle.removeNode(nodesCircle.getNodeById(18));
        nodesCircle.removeNode(nodesCircle.getNodeById(19));
        nodesCircle.removeNode(nodesCircle.getNodeById(2));
        nodesCircle.removeNode(nodesCircle.getNodeById(16));
        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(4)), Optional.of(2128658425));
    }

    @Test
    public void testFindPredecessorRingHash()  {
        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(1)), Optional.of(357678050));
        assertEquals(nodesCircle.findPredecessorRingHash(357678050), 348429519);
    }
}
