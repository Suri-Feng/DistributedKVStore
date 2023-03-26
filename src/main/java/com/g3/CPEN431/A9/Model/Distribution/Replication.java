package com.g3.CPEN431.A9.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A9.Model.Command;
import com.g3.CPEN431.A9.Model.ErrorCode;
import com.g3.CPEN431.A9.Model.Store.KVStore;
import com.g3.CPEN431.A9.Model.Store.StoreCache;
import com.g3.CPEN431.A9.Model.Store.Value;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.g3.CPEN431.A9.Utility.NetUtils.getChecksum;

public class Replication {
    private final DatagramSocket socket;
    private final ConcurrentHashMap<ByteString, WriteAcks> writeAckCache;
    private final KVStore store = KVStore.getInstance();
    private final StoreCache storeCache = StoreCache.getInstance();
    private final NodesCircle nodesCircle = NodesCircle.getInstance();
    KeyTransferManager keyTransferManager = KeyTransferManager.getInstance();

    private static final int TIME_OUT = 300; // Assume a message propagation time = 1 second
    public static final int DEFAULT_CACHE_SIZE = 150;

    public Replication(DatagramSocket socket) {
        this.socket = socket;
//        this.writeAckCache = Caffeine.newBuilder()
//                .maximumSize(DEFAULT_CACHE_SIZE)
//                .expireAfterWrite(TIME_OUT, TimeUnit.SECONDS)
//                .build();
        this.writeAckCache = new ConcurrentHashMap<>();
    }


    // Only primary can recv PUT, other recv BACKUP_WRITE
    public boolean isPrimary(ByteString key) {
        NodesCircle nodesCircle = NodesCircle.getInstance();
        return nodesCircle.findNodebyKey(key).getId() == nodesCircle.getThisNodeId();
    }

    public boolean isPrimaryOrBackup(ByteString key) {
        Node node = nodesCircle.findNodebyKey(key);
        if(node.getId() == nodesCircle.getThisNodeId()) return true;
        Set<Node> backupNodes = nodesCircle.findSuccessorNodes(nodesCircle.getCurrentNode());
        for (Node backupNode: backupNodes) {
            if(node.getId() == backupNode.getId()) return true;
        }
        return false;
    }

//    public void createWriteAckCache(ByteString messageID, InetAddress clientAddress, Integer clientPort) {
//        writeAckCache.put(messageID, new WriteAcks(clientAddress, clientPort));
//        //System.out.println("after put" + writeAckCache.size());
//    }

//    public void updateWriteAckCacheNEK(ByteString messageID) {
//        WriteAcks writeAcks = writeAckCache.get(messageID);
//        writeAckCache.remove(messageID);
//        sendNEKToClient(writeAcks.getClientAddress(), writeAcks.getClientPort(), messageID);
//    }

    // Only primary
    // Change to recv Ack
//    public void updateWriteAckCache(ByteString messageID) throws InvalidProtocolBufferException {
//        WriteAcks writeAcks = writeAckCache.get(messageID);
//
////        WriteAcks writeAcks = writeAckCache.getIfPresent(messageID);
//        if (writeAcks == null) {
//            System.out.println("=======Error in write ack cache" + writeAckCache.size());
//            return;
//        }
//
//        writeAcks.updateAck();
//        writeAckCache.put(messageID, writeAcks);
//
//        if (writeAcks.allBackupsAcked()) {
//            sendSuccessToClient(writeAcks.getClientAddress(), writeAcks.getClientPort(), messageID);
//            //System.out.println("all acked" + messageID);
//            writeAckCache.remove(messageID);
//        }
//    }


    public void takePrimaryPosition(Node deadPrimary) {
        List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
        List<KeyValueRequest.HashRange> hashRanges = nodesCircle.getRecoveredNodeRange(deadPrimary);
        for (Map.Entry<ByteString, Value> entry : store.getStore().entrySet()) {
            String sha256 = Hashing.sha256().hashBytes(entry.getKey().toByteArray()).toString();
            int ringHash = nodesCircle.getCircleBucketFromHash(sha256.hashCode());

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

        if(!allPairs.isEmpty()) {
            Set<Node> backupNodes = nodesCircle.findSuccessorNodes(nodesCircle.getCurrentNode());
            for (Node backupNode: backupNodes) {
                keyTransferManager.sendMessage(allPairs, backupNode);
            }
        }
    }

    // Send put/ rem
    public void sendWriteToBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID, Boolean remove) {
        Set<Node> backupNodes = nodesCircle.findSuccessorNodes(nodesCircle.getCurrentNode());

        for (Node backupNode: backupNodes) {

            KeyValueRequest.KVRequest request;

            if(remove)
            {
                request = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(Command.BACKUP_REM.getCode())
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
//                    .setClientAddress(ByteString.copyFrom(Objects.requireNonNull(writeAckCache.get(MessageID)).getClientAddress().getAddress()))
//                    .setClientPort(Objects.requireNonNull(writeAckCache.get(MessageID)).getClientPort())
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

//    public void sendAckToPrimary(InetAddress primaryAddress, Integer primaryPort, ByteString messageID) {
//        sendToPrimary(primaryAddress, primaryPort, messageID, Command.BACKUP_ACK);
//    }
//
//    public void sendNEKToPrimary(InetAddress primaryAddress, Integer primaryPort, ByteString messageID) {
//        sendToPrimary(primaryAddress, primaryPort, messageID, Command.BACKUP_NONEXISTENT_KEY);
//    }
//    public void sendToPrimary(InetAddress primaryAddress, Integer primaryPort, ByteString messageID, Command code) {
//        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
//                .setCommand(code.getCode())
//                .build();
//
//        Message.Msg requestMessage = Message.Msg.newBuilder()
//                .setMessageID(messageID)
//                .setPayload(request.toByteString())
//                .setCheckSum(getChecksum(messageID.toByteArray(), request.toByteArray()))
//                .build();
//
//        byte[] requestBytes = requestMessage.toByteArray();
//        DatagramPacket packet = new DatagramPacket(
//                requestBytes,
//                requestBytes.length,
//                primaryAddress,
//                primaryPort);
//
//        try {
//            socket.send(packet);
//        } catch (IOException e) {
//            System.out.println("====================");
//            System.out.println("[sendWriteAckToPrimary]" + e.getMessage());
//            System.out.println("====================");
//            throw new RuntimeException(e);
//        }
//    }

    // out-of-space - Can be sent by all -> called when handle BACKUP_WRITE
    public void sendOutOfSpaceToClient(InetAddress clientAddress, Integer clientPort, ByteString messageID) {
        Message.Msg responseMessage1 = storeCache.getCache().getIfPresent(ByteBuffer.wrap(messageID.toByteArray()));
        if (responseMessage1 != null) {
            System.out.println("Error - Should not store out-of-space in cache");
        }
        Message.Msg responseMessage = sendResponseToClient(clientAddress, clientPort, messageID, ErrorCode.OUT_OF_SPACE);
    }

    // success - Can be sent by primary only -> called by update write ack cache
//    public void sendSuccessToClient(InetAddress clientAddress, Integer clientPort, ByteString messageID) {
//        Message.Msg responseMessage1 = storeCache.getCache().getIfPresent(ByteBuffer.wrap(messageID.toByteArray()));
//        if (responseMessage1 == null) {
//            System.out.println("Error - Should have a PUT/REM in cache");
//        }
//        Message.Msg responseMessage = sendResponseToClient(clientAddress, clientPort, messageID, ErrorCode.SUCCESSFUL);
//        storeCache.getCache().put(ByteBuffer.wrap(messageID.toByteArray()), responseMessage);
//    }
//    public void sendNEKToClient(InetAddress clientAddress, Integer clientPort, ByteString messageID) {
//        Message.Msg responseMessage = sendResponseToClient(clientAddress, clientPort, messageID, ErrorCode.NONEXISTENT_KEY);
//        storeCache.getCache().put(ByteBuffer.wrap(messageID.toByteArray()), responseMessage);
//    }
    public Message.Msg sendResponseToClient(InetAddress clientAddress, Integer clientPort, ByteString messageID, ErrorCode code) {

        //System.out.println("Send msg to client, error code" + code);
        KeyValueResponse.KVResponse response = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(code.getCode())
                .build();

        Message.Msg responseMessage = Message.Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(response.toByteString())
                .setCheckSum(getChecksum(messageID.toByteArray(), response.toByteArray()))
                .build();

        byte[] responseBytes = responseMessage.toByteArray();
        DatagramPacket packet = new DatagramPacket(
                responseBytes,
                responseBytes.length,
                clientAddress,
                clientPort);

        try {
            socket.send(packet);
        } catch (IOException e) {
            System.out.println("====================");
            System.out.println("[sendResponseToClient]" + e.getMessage());
            System.out.println("====================");
            throw new RuntimeException(e);
        }
        return responseMessage;
    }
}
