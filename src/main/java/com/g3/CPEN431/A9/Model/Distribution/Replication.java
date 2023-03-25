package com.g3.CPEN431.A9.Model.Distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A9.Model.Command;
import com.g3.CPEN431.A9.Model.ErrorCode;
import com.g3.CPEN431.A9.Model.Store.KVStore;
import com.google.protobuf.ByteString;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.g3.CPEN431.A9.Utility.NetUtils.getChecksum;

public class Replication {
    private final DatagramSocket socket;
    private final ConcurrentHashMap<ByteString, WriteAcks> writeAckCache;
    private final KVStore store = KVStore.getInstance();
    private final NodesCircle nodesCircle = NodesCircle.getInstance();

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

    public void createWriteAckCache(ByteString messageID, InetAddress clientAddress, Integer clientPort) {
        writeAckCache.put(messageID, new WriteAcks(clientAddress, clientPort));
        //System.out.println("after put" + writeAckCache.size());
    }

    // Only primary
    public void updateWriteAckCache(ByteString messageID) {
        WriteAcks writeAcks = writeAckCache.get(messageID);

//        WriteAcks writeAcks = writeAckCache.getIfPresent(messageID);
//        if (writeAcks == null) {
//            System.out.println("Error in write ack cache" + writeAckCache.stats().loadCount());
//            return;
//        }
        writeAcks.updateAck();
        writeAckCache.put(messageID, writeAcks);

        if (writeAcks.allBackupsAcked()) {
            sendResponseToClient(writeAcks.getClientAddress(), writeAcks.getClientPort(), messageID, ErrorCode.SUCCESSFUL);
//            System.out.println("all acked" + messageID);
//            writeAckCache.remove(messageID);
        }
    }

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
                    .setClientAddress(ByteString.copyFrom(Objects.requireNonNull(writeAckCache.get(MessageID)).getClientAddress().getAddress()))
                    .setClientPort(Objects.requireNonNull(writeAckCache.get(MessageID)).getClientPort())
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

    public void sendAckToPrimary(InetAddress primaryAddress, Integer primaryPort, ByteString messageID) {
        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Command.BACKUP_ACK.getCode())
                .build();

        Message.Msg requestMessage = Message.Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(request.toByteString())
                .setCheckSum(getChecksum(messageID.toByteArray(), request.toByteArray()))
                .build();

        byte[] requestBytes = requestMessage.toByteArray();
        DatagramPacket packet = new DatagramPacket(
                requestBytes,
                requestBytes.length,
                primaryAddress,
                primaryPort);

        try {
            socket.send(packet);
        } catch (IOException e) {
            System.out.println("====================");
            System.out.println("[sendWriteAckToPrimary]" + e.getMessage());
            System.out.println("====================");
            throw new RuntimeException(e);
        }
    }
    public void sendResponseToClient(InetAddress clientAddress, Integer clientPort, ByteString messageID, ErrorCode code) {
        // out-of-space - Can be sent by all -> called when handle BACKUP_WRITE
        // success - Can be sent by primary only -> called by update write ack cache
        KeyValueResponse.KVResponse response = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(code.getCode())
                .build();
        byte[] responseBytes = response.toByteArray();
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
    }
}
