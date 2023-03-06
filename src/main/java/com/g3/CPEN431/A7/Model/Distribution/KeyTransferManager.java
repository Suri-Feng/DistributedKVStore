package com.g3.CPEN431.A7.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A7.Model.Command;
import com.g3.CPEN431.A7.Model.KVServer;
import com.g3.CPEN431.A7.Model.Store.KVStore;
import com.g3.CPEN431.A7.Model.Store.ValueV;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

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

    public List<ByteBuffer> transferKeys(Node recoveredNode) {
        if (socket == null) {
            return null;
        }
        List<ByteBuffer> keysToTransfer = new ArrayList<>();
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();

        // RecoveredNode Ring hash (only one rn which is the one before me)
        Integer maxHash = nodesCircle.getRingHashIfMyPredecessor(recoveredNode.getId());

        if (maxHash != null) {
            System.out.println("Recovered port " + recoveredNode.getPort() + " is a predecessor of port " + KVServer.port);
            int minHash = nodesCircle.findPredecessorRingHash(maxHash) + 1;
            for (Map.Entry<ByteBuffer, ValueV> entry: store.getStore().entrySet()) {
                String sha256 = Hashing.sha256()
                        .hashBytes(entry.getKey()).toString();
                int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());
                if (ringHash <= maxHash && ringHash >= minHash) {
                    keysToTransfer.add(entry.getKey());

                    allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                            .setVersion(entry.getValue().getVersion())
                            .setValue(entry.getValue().getValue())
                            .setKey(ByteString.copyFrom(entry.getKey().array()))
                            .build());
                }
            }
        }
        if (!allPairs.isEmpty()) {
            sendMessage(allPairs, recoveredNode);
        }
        return keysToTransfer;
    }

    private void sendMessage(List<KeyValueRequest.KeyValueEntry> allPairs, Node recoveredNode) {
        byte[] msg_id = new byte[0];

        System.out.println(KVServer.port + " sending key transfers to port "
                + recoveredNode.getPort()
                + " with # pairs: " + allPairs.size());

        for (KeyValueRequest.KeyValueEntry entry: allPairs) {
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
