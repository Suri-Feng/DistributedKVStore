package com.g3.CPEN431.A7.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A7.Model.Command;
import com.g3.CPEN431.A7.Model.KVServer;
import com.g3.CPEN431.A7.Model.Store.KVStore;
import com.g3.CPEN431.A7.Model.Store.Value;
import com.g3.CPEN431.A7.Utility.StringUtils;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KeyTransferManager {
    private DatagramSocket socket;
    private final KVStore store = KVStore.getInstance();
    private final NodesCircle nodesCircle = NodesCircle.getInstance();
    private final static KeyTransferManager instance = new KeyTransferManager();
    public static KeyTransferManager getInstance() {
        return instance;
    }
    public void setSocket(DatagramSocket socket) {
        this.socket = socket;
    }
    private KeyTransferManager() {
        this.socket = null;
    }

    public List<ByteString> transferKeys(Node recoveredNode) {
        List<ByteString> keysToTransfer = new ArrayList<>();
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();

        // RecoveredNode Ring hash (only one rn which is the one before me)
        ArrayList<Integer> maxHashes = nodesCircle.getRingHashIfMyPredecessor(recoveredNode.getId());

        if (!maxHashes.isEmpty()) {
            System.out.println("Recovered port " + recoveredNode.getPort() + " is a predecessor of port " + KVServer.port);
            for (Integer maxHash: maxHashes) {
                int minHash = nodesCircle.findPredecessorRingHash(maxHash) + 1;
                for (Map.Entry<ByteString, Value> entry: store.getStore().entrySet()) {
                    String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
                    int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

                    // keys within affected range
                    if (ringHash <= maxHash && ringHash >= minHash) {
                        keysToTransfer.add(entry.getKey());

                        allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                                .setVersion(entry.getValue().getVersion())
                                .setValue(entry.getValue().getValue())
                                .setKey(entry.getKey())
                                .build());
                    }
                }
            }
        } else {
            Set<Node> successorNodes = nodesCircle.findSuccessorNodes(recoveredNode);
            sendMessageToSuccessor(successorNodes, recoveredNode);
        }

//
//
//        int[][] minMax = nodesCircle.getRecoveredNodeRange(recoveredNode);
//
//        for (Map.Entry<ByteString, Value> entry: store.getStore().entrySet()) {
//            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
//            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());
//
//            // keys within affected range
//            if (ringHash <= minMax[0][1] && ringHash >= minMax[0][0] ||
//                    ringHash <= minMax[1][1] && ringHash >= minMax[1][0] ||
//                    ringHash <= minMax[2][1] && ringHash >= minMax[2][0]) {
//                keysToTransfer.add(entry.getKey());
//
//                allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
//                        .setVersion(entry.getValue().getVersion())
//                        .setValue(entry.getValue().getValue())
//                        .setKey(entry.getKey())
//                        .build());
//            }
//        }
        if (!allPairs.isEmpty()) {
            sendMessage(allPairs, recoveredNode);
        }
        return keysToTransfer;
    }

    private void sendMessageToSuccessor(Set<Node> successorNodes, Node recoveredNode) {
        byte[] msg_id = new byte[0];

        for (Node node: successorNodes) {
            KeyValueRequest.KVRequest req = KeyValueRequest.KVRequest.newBuilder()
                    .setCommand(Command.SUCCESSOR_NOTIFY.getCode())
                    .setRecoveredNodeId(recoveredNode.getId())
                    .build();

            // Create the message
            Message.Msg requestMessage = Message.Msg.newBuilder()
                    .setMessageID(ByteString.copyFrom(msg_id))
                    .setPayload(req.toByteString())
                    .setCheckSum(0)
                    .build();

            byte[] requestBytes = requestMessage.toByteArray();
            DatagramPacket packet = new DatagramPacket(
                    requestBytes,
                    requestBytes.length,
                    node.getAddress(),
                    node.getPort());

            try {
                socket.send(packet);
            } catch (IOException e) {
                System.out.println("====================");
                System.out.println(e.getMessage());
                System.out.println("====================");
                throw new RuntimeException(e);
            }
        }
    }

    private void sendMessage(List<KeyValueRequest.KeyValueEntry> allPairs, Node recoveredNode) {
        byte[] msg_id = new byte[0];

        for (KeyValueRequest.KeyValueEntry entry: allPairs) {
            System.out.println(KVServer.port + " sending key transfers to port "
                    + recoveredNode.getPort()
                    + " key: " + StringUtils.byteArrayToHexString(entry.getKey().toByteArray()));

            KeyValueRequest.KVRequest pairs = KeyValueRequest.KVRequest.newBuilder()
                    .setCommand(Command.KEY_TRANSFER.getCode())
                    .setPair(entry)
                    .build();

            // Create the message
            Message.Msg requestMessage = Message.Msg.newBuilder()
                    .setMessageID(ByteString.copyFrom(msg_id))
                    .setPayload(pairs.toByteString())
                    .setCheckSum(0)
                    .build();

            byte[] requestBytes = requestMessage.toByteArray();
            DatagramPacket packet = new DatagramPacket(
                    requestBytes,
                    requestBytes.length,
                    recoveredNode.getAddress(),
                    recoveredNode.getPort());

            try {
                socket.send(packet);
            } catch (IOException e) {
                System.out.println("====================");
                System.out.println(e.getMessage());
                System.out.println("====================");
                throw new RuntimeException(e);
            }
        }
    }
}
