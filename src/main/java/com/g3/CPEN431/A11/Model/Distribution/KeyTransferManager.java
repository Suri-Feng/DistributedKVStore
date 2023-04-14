package com.g3.CPEN431.A11.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A11.Model.Command;
import com.g3.CPEN431.A11.Model.Store.KVStore;
import com.g3.CPEN431.A11.Model.Store.Value;
import com.g3.CPEN431.A11.Utility.MemoryUsage;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KeyTransferManager {
    private DatagramSocket socket;
    KVStore store = KVStore.getInstance();
    NodesCircle nodesCircle = NodesCircle.getInstance();
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

    public void sendMessageToBackups(List<KeyValueRequest.KeyValueEntry> allPairs, Node backupNode) {
        byte[] msg_id = new byte[0];

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
                    backupNode.getAddress(),
                    backupNode.getPort());

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

    private boolean keyWithinRange(KeyValueRequest.HashRange hashRange, int ringHash) {
        if (hashRange.getMinRange() <= hashRange.getMaxRange()) {
            return (ringHash >= hashRange.getMinRange()) && (ringHash <= hashRange.getMaxRange());
        } else {
            return (ringHash <= hashRange.getMaxRange()) || (ringHash >= hashRange.getMinRange());
        }
    }

    public void updateBackupPosition(Node newBackup, int VN) {
        KeyValueRequest.HashRange hashRange = nodesCircle.getHashRangeByHash(VN, nodesCircle.getCurrentNode());
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
        for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

//            if (keyWithinRange(hashRange, ringHash) && isPrimary(entry.getKey()))
            if (keyWithinRange(hashRange, ringHash))
                allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                        .setVersion(entry.getValue().getVersion())
                        .setValue(entry.getValue().getValue())
                        .setKey(entry.getKey())
                        .setRTS(entry.getValue().getR_TS())
                        .build());
        }
        sendMessageToBackups(allPairs, newBackup);
    }

    public void recoverPrimaryPosition(Node recoveredPrimary, List<KeyValueRequest.HashRange> hashRanges) {
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();

        // I am only responsible for sending to the range with maxRange just pred to my VN(s)
        for (KeyValueRequest.HashRange range : hashRanges) {
            for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
                String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
                int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());
//                if (keyWithinRange(range, ringHash) && isPrimary(entry.getKey()))
                if (keyWithinRange(range, ringHash))
                    allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                            .setVersion(entry.getValue().getVersion())
                            .setValue(entry.getValue().getValue())
                            .setKey(entry.getKey())
                            .setRTS(entry.getValue().getR_TS())
                            .build());

            }
        }

        sendMessagePrimaryRecover(allPairs, recoveredPrimary);
    }

    //If my predecessor dead, I will take the primary postion
    // I will need my predecessor's place on the ring, before remove it
    public void takePrimaryPosition(List<KeyValueRequest.HashRange> hashRanges) {
        ConcurrentHashMap<Node, List<KeyValueRequest.KeyValueEntry>> allPairsForNode = new ConcurrentHashMap<>();

        for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

            for (KeyValueRequest.HashRange range : hashRanges) {
                if (keyWithinRange(range, ringHash)) {

                    int VN = nodesCircle.findSuccVNbyRingHash(ringHash);

                    for (Node currentBackup : nodesCircle.getMySuccessors().get(VN).values()) {

                        if (!allPairsForNode.containsKey(currentBackup))
                            allPairsForNode.put(currentBackup, new ArrayList<>());
                        allPairsForNode.get(currentBackup).add(KeyValueRequest.KeyValueEntry.newBuilder()
                                .setVersion(entry.getValue().getVersion())
                                .setValue(entry.getValue().getValue())
                                .setKey(entry.getKey())
                                .setRTS(entry.getValue().getR_TS())
                                .build());

                    }
                }
            }
        }

        for (Node newBackupNode : allPairsForNode.keySet()) {
            sendMessageToBackups(allPairsForNode.get(newBackupNode), newBackupNode);
        }
    }

    public void sendREMtoBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID, long R_TS) {
        sendWriteToBackups(reqPayload, MessageID, true, R_TS);
    }


    public void sendPUTtoBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID, long R_TS) {
        sendWriteToBackups(reqPayload, MessageID, false, R_TS);
    }

    private void sendWriteToBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID, Boolean remove, long R_TS) {
        if (nodesCircle.getStartupNodesSize() == 1) return;
        ByteString key = reqPayload.getKey();
        String sha256 = Hashing.sha256().hashBytes(key.toByteArray()).toString();

        int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());
        int VN = nodesCircle.findSuccVNbyRingHash(ringHash);

        for (Node backupNode : nodesCircle.getMySuccessors().get(VN).values()) {

            KeyValueRequest.KVRequest request;

            if (remove) {
                request = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(Command.BACKUP_REM.getCode())
                        .setKey(reqPayload.getKey())
                        .build();
            } else {
                request = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(Command.BACKUP_WRITE.getCode())
                        .setKey(reqPayload.getKey())
                        .setValue(reqPayload.getValue())
                        .setVersion(reqPayload.getVersion())
                        .setRTS(R_TS)
                        .build();
            }

            Message.Msg requestMessage = Message.Msg.newBuilder()
                    .setMessageID(MessageID)
                    .setPayload(request.toByteString())
                    .setCheckSum(0)
                    .build();

            byte[] requestBytes = requestMessage.toByteArray();
            DatagramPacket packet = new DatagramPacket(
                    requestBytes,
                    requestBytes.length,
                    backupNode.getAddress(),
                    backupNode.getPort());

            try {
                socket.send(packet);
            } catch (IOException e) {
                System.out.println("====================");
                System.out.println("[sendWriteToBackups]" + e.getMessage());
                System.out.println("====================");
                throw new RuntimeException(e);
            }
        }
    }



    public void handleReplicationRequest(KeyValueRequest.KVRequest reqPayload) {
        int command = reqPayload.getCommand();

        if  (command == Command.KEY_TRANSFER.getCode()) {
            addKey(reqPayload.getPair());
            return;
        }

        if (command == Command.PRIMARY_RECOVER.getCode()) {
            addKeyPrimaryRecover(reqPayload.getPair());
            return;
        }

        if (command == Command.BACKUP_REM.getCode()) {
            backupREM(reqPayload);
            return;
        }
    }

    public void backupPUT(KeyValueRequest.KVRequest requestPayload, ByteString id, InetAddress address
            , int port)  {
        if (isMemoryOverload()) {
            return;
        }
        Value inStoreValue = store.getStore().get(requestPayload.getKey());
        if (inStoreValue != null && inStoreValue.getR_TS() > requestPayload.getRTS()) {
            return;
        }

        putValueInStore(requestPayload.getKey(), requestPayload.getValue(), requestPayload.getVersion(), requestPayload.getRTS());

        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Command.PUT_ACK.getCode())
                .build();

        Message.Msg requestMessage = Message.Msg.newBuilder()
                .setMessageID(id)
                .setPayload(request.toByteString())
                .setCheckSum(0)
                .build();

        byte[] requestBytes = requestMessage.toByteArray();
        DatagramPacket packet = new DatagramPacket(
                requestBytes,
                requestBytes.length,
                address,
                port);

        try {
            socket.send(packet);
        } catch (IOException e) {
            System.out.println("====================");
            System.out.println("[sendACKtoPrimary]" + e.getMessage());
            System.out.println("====================");
            throw new RuntimeException(e);
        }
    }

    private void backupREM(KeyValueRequest.KVRequest requestPayload) {
        store.getStore().remove(requestPayload.getKey());
    }

    private void addKey(KeyValueRequest.KeyValueEntry pair) {
        Value inStoreValue = store.getStore().get(pair.getKey());
        if (inStoreValue != null && inStoreValue.getR_TS() > pair.getRTS()) {
            return;
        }
        Value value = new Value(pair.getVersion(), pair.getValue(), pair.getRTS());
        store.getStore().put(
                pair.getKey(),
                value);
    }

    private void addKeyPrimaryRecover(KeyValueRequest.KeyValueEntry pair) {
        if (store.getStore().containsKey(pair.getKey()) && store.getStore().get(pair.getKey()).getR_TS() > pair.getRTS()) {
            return;
        }

        Value value = new Value(pair.getVersion(), pair.getValue(), pair.getRTS());

        store.getStore().put(
                pair.getKey(),
                value);

        byte[] key = pair.getKey().toByteArray();
        String sha256 = Hashing.sha256()
                .hashBytes(key).toString();
        Node nodeMatch = nodesCircle.findCorrectNodeByHash(sha256.hashCode());

        if (nodesCircle.getAliveNodesList().containsValue(nodeMatch) && nodeMatch.getId() != nodesCircle.getThisNodeId()) {
            List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
            allPairs.add(pair);
            sendMessagePrimaryRecover(allPairs, nodeMatch);
        }

//        for (ConcurrentHashMap<Integer, Node> pred: nodesCircle.getMyPredessors().values()) {
//            if (pred.containsValue(nodeMatch)) {
//                List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
//                allPairs.add(pair);
//                sendMessagePrimaryRecover(allPairs, nodeMatch);
//            }
//        }


    }

    private void putValueInStore(ByteString key, ByteString value, int version, long R_TS) {
        store.getStore().put(key, new Value(version, value, R_TS));
    }

    private boolean isMemoryOverload() {
        if (nodesCircle.getStartupNodesSize() == 1) {
            return MemoryUsage.getFreeMemory() < 0.035 * MemoryUsage.getMaxMemory();
        } else {
            return MemoryUsage.getFreeMemory() < 0.04 * MemoryUsage.getMaxMemory();
        }
    }

    public boolean isPrimary(ByteString key) {
        return nodesCircle.findNodebyKey(key).getId() == nodesCircle.getThisNodeId();
    }

    /*
     * 1. Get the tail map of the key ring hash
     * 2. If tail map is empty, the first node in the ring is primary, find 3 unique immediate successors different from primary
     * 3. If not empty, the first node in the tail map is primary, as long as tail map still has entries, find successors from tail map, otherwise find from circle
     * This doesn't consider the case where the circle has less than 4 nodes
     */

}
