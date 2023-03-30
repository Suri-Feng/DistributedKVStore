package com.g3.CPEN431.A9.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A9.Model.Command;
import com.g3.CPEN431.A9.Model.KVServer;
import com.g3.CPEN431.A9.Model.Store.KVStore;
import com.g3.CPEN431.A9.Model.Store.Value;
import com.g3.CPEN431.A9.Utility.StringUtils;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

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

    public List<KeyValueRequest.KeyValueEntry> transferKeysWithinRange(Node recoveredNode, List<KeyValueRequest.HashRange> hashRanges) {
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();

        for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();

            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());
            // keys within affected range
//            System.out.println("Key hash" + StringUtils.byteArrayToHexString(entry.getKey().toByteArray()) + ", key range" + ringHash);
//            System.out.println("range 0 " + hashRanges.get(0).getMinRange() + " ," + hashRanges.get(0).getMaxRange());
//            System.out.println("range 1 " + hashRanges.get(1).getMinRange() + " ," + hashRanges.get(1).getMaxRange());
//            System.out.println("range 2 " + hashRanges.get(2).getMinRange() + " ," + hashRanges.get(2).getMaxRange());
            if ((ringHash <= hashRanges.get(0).getMaxRange() && ringHash >= hashRanges.get(0).getMinRange()) ||
                    (ringHash <= hashRanges.get(1).getMaxRange() && ringHash >= hashRanges.get(1).getMinRange()) ||
                    (ringHash <= hashRanges.get(2).getMaxRange() && ringHash >= hashRanges.get(2).getMinRange())) {

                allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                        .setVersion(entry.getValue().getVersion())
                        .setValue(entry.getValue().getValue())
                        .setKey(entry.getKey())
                        .build());
            }
        }

        if (!allPairs.isEmpty()) {
            sendMessage(allPairs, recoveredNode);
//            System.out.println(KVServer.port + "sending" + allPairs.size() + " keys for " + recoveredNode.getPort());
        } else {
//            System.out.println(KVServer.port + "has no keys for " + recoveredNode.getPort());
        }
        return allPairs;
    }

    public void sendMessage(List<KeyValueRequest.KeyValueEntry> allPairs, Node recoveredNode) {
        byte[] msg_id = new byte[0];

        for (KeyValueRequest.KeyValueEntry entry: allPairs) {
//            System.out.println(KVServer.port + " sending key transfers to port "
//                    + recoveredNode.getPort()
//                    + " key: " + StringUtils.byteArrayToHexString(entry.getKey().toByteArray()));

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

    public void sendMessagePrimaryRecover(List<KeyValueRequest.KeyValueEntry> allPairs, Node recoveredNode) {
        byte[] msg_id = new byte[0];

        for (KeyValueRequest.KeyValueEntry entry: allPairs) {
//            System.out.println(KVServer.port + " sending key transfers to port "
//                    + recoveredNode.getPort()
//                    + " key: " + StringUtils.byteArrayToHexString(entry.getKey().toByteArray()));

            KeyValueRequest.KVRequest pairs = KeyValueRequest.KVRequest.newBuilder()
                    .setCommand(Command.PRIMARY_RECOVER.getCode())
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

    /*
     * 1. Get the tail map of the key ring hash
     * 2. If tail map is empty, the first node in the ring is primary, find 3 unique immediate successors different from primary
     * 3. If not empty, the first node in the tail map is primary, as long as tail map still has entries, find successors from tail map, otherwise find from circle
     * This doesn't consider the case where the circle has less than 4 nodes
     */

}
