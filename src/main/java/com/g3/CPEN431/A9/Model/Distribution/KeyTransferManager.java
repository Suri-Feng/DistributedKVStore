package com.g3.CPEN431.A9.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A9.Model.Command;
import com.g3.CPEN431.A9.Model.KVServer;
import com.g3.CPEN431.A9.Model.Store.KVStore;
import com.g3.CPEN431.A9.Model.Store.Value;
import com.g3.CPEN431.A9.Utility.MemoryUsage;
import com.g3.CPEN431.A9.Utility.StringUtils;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.g3.CPEN431.A9.Model.KVServer.port;
import static com.g3.CPEN431.A9.Utility.NetUtils.getChecksum;

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



    private boolean keyWithinRange(KeyValueRequest.HashRange hashRange, int ringHash) {
        if(hashRange.getMinRange() <= hashRange.getMaxRange()) {
            return (ringHash >= hashRange.getMinRange()) && (ringHash <= hashRange.getMaxRange());
        }else{
            return (ringHash <= hashRange.getMaxRange()) || (ringHash >= hashRange.getMinRange());
        }
    }

    public void updateBackupPosition(Node newBackup, int VN) {
//        System.out.println(KVServer.port + "sent key transfer to backup" + newBackup.getPort());
        // List<KeyValueRequest.HashRange> hashRanges = nodesCircle.getRecoveredNodeRange(nodesCircle.getCurrentNode());
        KeyValueRequest.HashRange hashRange = nodesCircle.getHashRangeByHash(VN, nodesCircle.getCurrentNode());
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
        for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());


            if (keyWithinRange(hashRange, ringHash) && isPrimary(entry.getKey()))
                allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                        .setVersion(entry.getValue().getVersion())
                        .setValue(entry.getValue().getValue())
                        .setKey(entry.getKey())
                        .build());
        }
        sendMessageToBackups(allPairs, newBackup);
    }

    public void recoverPrimaryPosition(Node recoveredPrimary, List<KeyValueRequest.HashRange> hashRanges) {
//        System.out.println(KVServer.port + "sent key transfer to recovered primary" + recoveredPrimary.getPort());
//        NavigableSet<Integer> myVNSet =  nodesCircle.getMySuccessors().keySet();
//        List<KeyValueRequest.HashRange> hashRanges = nodesCircle.getRecoveredNodeRange(recoveredPrimary);
//        int recoveredPrimaryVN = nodesCircle.getPrevRingHash(VN);
//        KeyValueRequest.HashRange hashRange = nodesCircle.getHashRangeByHash(recoveredPrimaryVN, recoveredPrimary);
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();

        // TODO: Instead of sending all keys within 3 ranges
        // I am only responsible for sending to the range with maxRange just pred to my VN(s)
        for (KeyValueRequest.HashRange range : hashRanges) {
//            if (!myVNSet.contains(nodesCircle.getNextRingHash(range.getMaxRange()))) continue;

            for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
                String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
                int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

                if (keyWithinRange(range, ringHash)) {
                    allPairs.add(KeyValueRequest.KeyValueEntry.newBuilder()
                            .setVersion(entry.getValue().getVersion())
                            .setValue(entry.getValue().getValue())
                            .setKey(entry.getKey())
                            .build());
                }
            }
        }

        sendMessagePrimaryRecover(allPairs, recoveredPrimary);
//        for (Node backupNode : nodesCircle.findSuccessorNodesHashMap(recoveredPrimary).values()) {
//            if (backupNode != nodesCircle.getCurrentNode()) {
//                keyTransferManager.sendMessage(allPairs, backupNode);
//            }
//        }
    }

    //If my predecessor dead, I will take the primary postion
    // I will need my predecessor's place on the ring, before remove it
    public void takePrimaryPosition(List<KeyValueRequest.HashRange> hashRanges) {
        ConcurrentHashMap<Node, List<KeyValueRequest.KeyValueEntry>> allPairsForNode = new ConcurrentHashMap<>();
        //hashRanges.addAll(nodesCircle.getRecoveredNodeRange(nodesCircle.getCurrentNode()));

        for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

            for(KeyValueRequest.HashRange range: hashRanges) {
                // Can only send keys that I am primary, will not responsible for sending keys if not primary though I have them
                if (keyWithinRange(range, ringHash) && isPrimary(entry.getKey())) {


                    int VN = nodesCircle.findSuccVNbyRingHash(ringHash);

                    // TODO: Only need to pass to one succ (now pass to three), but this is not the priority for now
                    for(Node currentBackup: nodesCircle.getMySuccessors().get(VN).values()) {

                        if (!allPairsForNode.containsKey(currentBackup))
                            allPairsForNode.put(currentBackup, new ArrayList<>());
                        allPairsForNode.get(currentBackup).add(KeyValueRequest.KeyValueEntry.newBuilder()
                                .setVersion(entry.getValue().getVersion())
                                .setValue(entry.getValue().getValue())
                                .setKey(entry.getKey())
                                .build());

                    }
                }
            }
        }

        for(Node newBackupNode: allPairsForNode.keySet()) {
            sendMessageToBackups(allPairsForNode.get(newBackupNode), newBackupNode);
        }
    }

    public void sendREMtoBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID) {
        sendWriteToBackups(reqPayload, MessageID, true);
    }


    public void sendPUTtoBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID) {
        sendWriteToBackups(reqPayload, MessageID, false);
    }

    private void sendWriteToBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID, Boolean remove) {
        if(nodesCircle.getStartupNodesSize() == 1) return;
        ByteString key = reqPayload.getKey();
        String sha256 = Hashing.sha256().hashBytes(key.toByteArray()).toString();

        int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());
        int VN = nodesCircle.findSuccVNbyRingHash(ringHash);

        for (Node backupNode: nodesCircle.getMySuccessors().get(VN).values()) {

            KeyValueRequest.KVRequest request;

            if(remove)
            {
                request = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(Command.BACKUP_REM.getCode())
                        .setKey(reqPayload.getKey())
                        .build();
            }
            else
            {
                request = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(Command.BACKUP_WRITE.getCode())
                        .setKey(reqPayload.getKey())
                        .setValue(reqPayload.getValue())
                        .setVersion(reqPayload.getVersion())
                        .build();
            }

            Message.Msg requestMessage = Message.Msg.newBuilder()
                    .setMessageID(MessageID)
                    .setPayload(request.toByteString())
                    .setCheckSum(getChecksum(MessageID.toByteArray(), request.toByteArray()))
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

    public void handleReplicationRequest(KeyValueRequest.KVRequest reqPayload, int port) {
        int command = reqPayload.getCommand();

        if  (command == Command.KEY_TRANSFER.getCode()) {
            String val = StringUtils.byteArrayToHexString(reqPayload.getPair().getValue().toByteArray());
            val = val.length() > 100 ? val.substring(0, 100): val;
            System.out.println(KVServer.port + " received key transfer (as backup) from "  + port +": " + StringUtils.byteArrayToHexString(reqPayload.getPair().getKey().toByteArray())
                    + ", val " + val
                    + ", version " + reqPayload.getPair().getVersion());
            addKey(reqPayload.getPair());
            return;
        }

        if (command == Command.PRIMARY_RECOVER.getCode()) {
            String val = StringUtils.byteArrayToHexString(reqPayload.getPair().getValue().toByteArray());
            val = val.length() > 100 ? val.substring(0, 100): val;
            System.out.println(KVServer.port + " received key transfer (primary) from "  + port +": " + StringUtils.byteArrayToHexString(reqPayload.getPair().getKey().toByteArray())
                    + ", val " + val
                    + ", version " + reqPayload.getPair().getVersion());
            addKeyPrimaryRecover(reqPayload.getPair());
            return;
        }

        if (command == Command.BACKUP_WRITE.getCode()) {
//            System.out.println(KVServer.port + " received backup put: " + StringUtils.byteArrayToHexString(reqPayload.getKey().toByteArray()));
            backupPUT(reqPayload);
            return;
        }

        if (command == Command.BACKUP_REM.getCode()) {
            backupREM(reqPayload);
            return;
        }
    }

    private void backupPUT(KeyValueRequest.KVRequest requestPayload)  {
        if (isMemoryOverload()) {
            System.out.println(KVServer.port + " BACKUP PUT OVERLOAD");
            return;
        }
        Value valueV = new Value(requestPayload.getVersion(), requestPayload.getValue());
        store.getStore().put(requestPayload.getKey(), valueV);

    }

    private void backupREM(KeyValueRequest.KVRequest requestPayload) {
        if (store.getStore().get(requestPayload.getKey()) != null) {
            return;
        }
        store.getStore().remove(requestPayload.getKey());
    }

    private void addKey(KeyValueRequest.KeyValueEntry pair) {
        store.getStore().put(
                pair.getKey(),
                new Value(pair.getVersion(), pair.getValue()));
    }

    private void addKeyPrimaryRecover(KeyValueRequest.KeyValueEntry pair) {
        store.getStore().put(
                pair.getKey(),
                new Value(pair.getVersion(), pair.getValue()));

        byte[] key = pair.getKey().toByteArray();
        String sha256 = Hashing.sha256()
                .hashBytes(key).toString();
        Node nodeMatch = nodesCircle.findCorrectNodeByHash(sha256.hashCode());

        for (ConcurrentHashMap<Integer, Node> pred: nodesCircle.getMyPredessors().values()) {
            if (pred.containsValue(nodeMatch)) {
                List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
                allPairs.add(pair);
                sendMessagePrimaryRecover(allPairs, nodeMatch);
            }
        }

//        if(nodesCircle.getAliveNodesList().containsValue(nodeMatch) && nodeMatch.getId() != nodesCircle.getThisNodeId())
//        {
//            //System.out.println("send to " + nodeMatch.getPort());
//            List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
//            allPairs.add(pair);
//            sendMessagePrimaryRecover(allPairs, nodeMatch);
//        }

    }

    private void addKeyPrimaryRecoverSecondary(KeyValueRequest.KeyValueEntry pair) {
        store.getStore().put(
                pair.getKey(),
                new Value(pair.getVersion(), pair.getValue()));
    }

    private boolean isMemoryOverload() {
        return MemoryUsage.getFreeMemory() < 0.04 * MemoryUsage.getMaxMemory();
    }

    public boolean isPrimary(ByteString key) {
        NodesCircle nodesCircle = NodesCircle.getInstance();
        return nodesCircle.findNodebyKey(key).getId() == nodesCircle.getThisNodeId();
    }

    /*
     * 1. Get the tail map of the key ring hash
     * 2. If tail map is empty, the first node in the ring is primary, find 3 unique immediate successors different from primary
     * 3. If not empty, the first node in the tail map is primary, as long as tail map still has entries, find successors from tail map, otherwise find from circle
     * This doesn't consider the case where the circle has less than 4 nodes
     */

}
