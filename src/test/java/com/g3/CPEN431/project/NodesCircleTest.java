package com.g3.CPEN431.project;

import com.g3.CPEN431.A11.App;
import com.g3.CPEN431.A11.Model.Distribution.Node;
import com.g3.CPEN431.A11.Model.Distribution.NodesCircle;
import com.g3.CPEN431.A11.Utility.StringUtils;
import com.google.common.hash.Hashing;
import org.junit.*;
import java.io.IOException;

public class NodesCircleTest {
    NodesCircle nodesCircle = NodesCircle.getInstance();

    Node n12347 = null;
    Node n12348 = null;
    Node n12349 = null;
    Node n12350 = null;
    Node n12351 = null;
    Node n12352 = null;
    Node n12353 = null;
    Node n12354 = null;
    Node n12355 = null;
    Node n12356 = null;
    Node n12357 = null;

    Node n12358 = null;
    Node n12359 = null;
    Node n12360 = null;
    Node n12361 = null;

    Node n12362 = null;
    Node n12363 = null;
    Node n12364 = null;
    Node n12365 = null;
    Node n12366 = null;
    Node n12367 = null;

    public NodesCircleTest() throws IOException {
        nodesCircle.setNodeList(App.parseNodes());
        nodesCircle.buildHashCircle();
        n12347 = nodesCircle.getNodeById(0);
         n12348 = nodesCircle.getNodeById(1);
         n12349 = nodesCircle.getNodeById(2);
         n12350 = nodesCircle.getNodeById(3);
         n12351 = nodesCircle.getNodeById(4);
         n12352 = nodesCircle.getNodeById(5);
         n12353 = nodesCircle.getNodeById(6);
         n12354 = nodesCircle.getNodeById(7);
         n12355 = nodesCircle.getNodeById(8);
         n12356 = nodesCircle.getNodeById(9);
         n12357 = nodesCircle.getNodeById(10);

         n12358 = nodesCircle.getNodeById(11);
         n12359 = nodesCircle.getNodeById(12);
         n12360 = nodesCircle.getNodeById(13);
         n12361 = nodesCircle.getNodeById(14);

         n12362 = nodesCircle.getNodeById(15);
         n12363 = nodesCircle.getNodeById(16);
         n12364 = nodesCircle.getNodeById(17);
         n12365 = nodesCircle.getNodeById(18);
         n12366 = nodesCircle.getNodeById(19);
        n12367 = nodesCircle.getNodeById(20);
    }
//
//    @Test
//    public void testCircleBuild() throws UnknownHostException {
//        assertEquals(nodesCircle.getNodeById(0).getPort(), 12347);
//        assertEquals(nodesCircle.getThisNodeId(), 0);
//    }
//
//    @Test
//    public void testGetRingHashIfMyPredecessor() {
//        assertNull(nodesCircle.getRingHashIfMyPredecessor(0));
//        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(1)), Optional.of(357678050));
//        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(9)), Optional.of(1411844308));
//        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(6)), Optional.of(1907984985));
//        nodesCircle.removeNode(nodesCircle.getNodeById(1));
//        assertNull(nodesCircle.getRingHashIfMyPredecessor(1));
//        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(12)), Optional.of(348429519));
//
//        nodesCircle.removeNode(nodesCircle.getNodeById(12));
//        assertNull(nodesCircle.getRingHashIfMyPredecessor(12));
//        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(16)), Optional.of(323137670));
//
//        nodesCircle.removeNode(nodesCircle.getNodeById(16));
//        nodesCircle.removeNode(nodesCircle.getNodeById(14));
//        nodesCircle.removeNode(nodesCircle.getNodeById(15));
//        nodesCircle.removeNode(nodesCircle.getNodeById(18));
//        nodesCircle.removeNode(nodesCircle.getNodeById(19));
//        nodesCircle.removeNode(nodesCircle.getNodeById(2));
//        nodesCircle.removeNode(nodesCircle.getNodeById(16));
//        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(4)), Optional.of(2128658425));
//    }

//    @Test
//    public void testFindPredecessorRingHash()  {
//        assertEquals(Optional.ofNullable(nodesCircle.getRingHashIfMyPredecessor(1)), Optional.of(357678050));
//        assertEquals(nodesCircle.findPredecessorRingHash(357678050), 348429519);
//    }

    @Test
    public void testReroute()  {
//        byte[] key = StringUtils.hexStringToByteArray("38880000");
//        String sha256 = Hashing.sha256().hashBytes(key).toString();
//        Node node = nodesCircle.findCorrectNodeByHash(sha256.hashCode());
//        System.out.println(node.getPort() + "!!!!!");
//        assertEquals(node.getPort(), 12349);
//
//        nodesCircle.removeNode(n12356);
//        nodesCircle.removeNode(n12359);
//        nodesCircle.removeNode(n12358);
//        nodesCircle.removeNode(n12363);
//        nodesCircle.removeNode(n12364);
//        nodesCircle.removeNode(n12349);
//        nodesCircle.removeNode(n12355);
//
//        nodesCircle.removeNode(n12357);
//        nodesCircle.removeNode(n12353);
//        nodesCircle.removeNode(n12362);
//
//
//        nodesCircle.rejoinNode(n12356);
//        nodesCircle.rejoinNode(n12359);
//        nodesCircle.rejoinNode(n12358);
//        nodesCircle.rejoinNode(n12363);
//        nodesCircle.rejoinNode(n12364);
//        nodesCircle.rejoinNode(n12349);
//        nodesCircle.rejoinNode(n12355);
//
//        node = nodesCircle.findCorrectNodeByHash(sha256.hashCode());
//        System.out.println(node.getPort() + "!!!!!");
//        assertEquals(node.getPort(), 12349);
    }

    @Test
    public void testKeyTransfer() {
        nodesCircle.setThisNodeId(n12360.getId());

        byte[] key = StringUtils.hexStringToByteArray("F3BD0000");
        String sha256 = Hashing.sha256().hashBytes(key).toString();
        Node node = nodesCircle.findCorrectNodeByHash(sha256.hashCode());
        System.out.println(node.getPort() + "!!!!!");

        nodesCircle.removeNode(n12347);
//        nodesCircle.removeNode(n12348);
//        nodesCircle.removeNode(n12349);
//        nodesCircle.removeNode(n12350);
//        nodesCircle.removeNode(n12351);
//        nodesCircle.removeNode(n12352);
//        nodesCircle.removeNode(n12353);
//        nodesCircle.removeNode(n12354);
//        nodesCircle.removeNode(n12355);
//        nodesCircle.removeNode(n12356);

         node = nodesCircle.findCorrectNodeByHash(sha256.hashCode());
        System.out.println(node.getPort() + "!!!!!");

        nodesCircle.rejoinNode(n12365);
        nodesCircle.rejoinNode(n12352);
        nodesCircle.rejoinNode(n12360);
        nodesCircle.rejoinNode(n12357);
        nodesCircle.rejoinNode(n12358);


//        System.out.println(maxHash);
//        ArrayList<Node> nodes = nodesCircle.findCorrectNodeByHashndSuccessorNodes(n12354);
//        nodes.forEach(node1 -> System.out.println(node1.getPort()));
//        int minHash = nodesCircle.findPredecessorRingHash(maxHash) + 1;
//        System.out.println(minHash);
//        int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());
//        System.out.println(ringHash);
//        if (ringHash <= maxHash && ringHash >= minHash) {
//            System.out.println("yes");
//        }

//        int[][] array = nodesCircle.getRecoveredNodeRange(n12354);
//        for (int i = 0; i < array.length; i++) {
//            // Iterate over each column
//            for (int j = 0; j < array[i].length; j++) {
//                System.out.print(array[i][j] + " ");
//            }
//            // Print a new line after each row
//            System.out.println();
//        }
    }
}
