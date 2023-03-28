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
    private final KVStore store = KVStore.getInstance();
    private final StoreCache storeCache = StoreCache.getInstance();
    private final NodesCircle nodesCircle = NodesCircle.getInstance();
    KeyTransferManager keyTransferManager = KeyTransferManager.getInstance();


    public Replication(DatagramSocket socket) {
        this.socket = socket;
    }


    // Only primary can recv PUT, other recv BACKUP_WRITE
    public boolean isPrimary(ByteString key) {
        NodesCircle nodesCircle = NodesCircle.getInstance();
        return nodesCircle.findNodebyKey(key).getId() == nodesCircle.getThisNodeId();
    }

//    public boolean isPrimaryOrBackup(ByteString key) {
//        Node node = nodesCircle.findNodebyKey(key);
//        if(node.getId() == nodesCircle.getThisNodeId()) return true;
//        Set<Node> backupNodes = nodesCircle.findSuccessorNodes(nodesCircle.getCurrentNode());
//        for (Node backupNode: backupNodes) {
//            if(node.getId() == backupNode.getId()) return true;
//        }
//        return false;
//    }
//
//    // Send put/ rem
//    public void sendWriteToBackups(KeyValueRequest.KVRequest reqPayload, ByteString MessageID, Boolean remove) {
//        Set<Node> backupNodes = nodesCircle.findSuccessorNodes(nodesCircle.getCurrentNode());
//
//        for (Node backupNode: backupNodes) {
//
//            KeyValueRequest.KVRequest request;
//
//            if(remove)
//            {
//                request = KeyValueRequest.KVRequest.newBuilder()
//                        .setCommand(Command.BACKUP_REM.getCode())
//                        .build();
//            }
//            else
//            {
//                request = KeyValueRequest.KVRequest.newBuilder()
//                        .setCommand(Command.BACKUP_WRITE.getCode())
//                        .setKey(reqPayload.getKey())
//                        .setValue(reqPayload.getValue())
//                        .setVersion(reqPayload.getVersion())
//                        .build();
//            }
//
//            Message.Msg requestMessage = Message.Msg.newBuilder()
//                    .setMessageID(MessageID)
//                    .setPayload(request.toByteString())
//                    .setCheckSum(getChecksum(MessageID.toByteArray(), request.toByteArray()))
////                    .setClientAddress(ByteString.copyFrom(Objects.requireNonNull(writeAckCache.get(MessageID)).getClientAddress().getAddress()))
////                    .setClientPort(Objects.requireNonNull(writeAckCache.get(MessageID)).getClientPort())
//                    .build();
//
//            byte[] requestBytes = requestMessage.toByteArray();
//            DatagramPacket packet = new DatagramPacket(
//                    requestBytes,
//                    requestBytes.length,
//                    backupNode.getAddress(),
//                    backupNode.getPort());
//
//            try {
//                socket.send(packet);
//            } catch (IOException e) {
//                System.out.println("====================");
//                System.out.println("[sendWriteToBackups]" + e.getMessage());
//                System.out.println("====================");
//                throw new RuntimeException(e);
//            }
//        }
//    }

}
