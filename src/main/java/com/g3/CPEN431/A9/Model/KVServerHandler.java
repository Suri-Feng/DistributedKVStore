package com.g3.CPEN431.A9.Model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A9.Model.Distribution.*;
import com.g3.CPEN431.A9.Utility.MemoryUsage;
import com.g3.CPEN431.A9.Model.Store.KVStore;
import com.g3.CPEN431.A9.Model.Store.StoreCache;
import com.g3.CPEN431.A9.Model.Store.Value;
import com.g3.CPEN431.A9.Utility.StringUtils;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;

import com.g3.CPEN431.A9.Model.Distribution.KeyTransferManager;

public class KVServerHandler implements Runnable {
    DatagramSocket socket;
    Message.Msg requestMessage;
    InetAddress address;
    int port;
    KVStore store = KVStore.getInstance();
    StoreCache storeCache = StoreCache.getInstance();
    NodesCircle nodesCircle = NodesCircle.getInstance();
    HeartbeatsManager heartbeatsManager = HeartbeatsManager.getInstance();
    KeyTransferManager keyTransferManager = KeyTransferManager.getInstance();
    Replication replication;

    KVServerHandler(Message.Msg requestMessage,
                    DatagramSocket socket,
                    InetAddress address,
                    int port,
                    Replication replication
    ) {
        this.socket = socket;
        this.requestMessage = requestMessage;
        this.address = address;
        this.port = port;
        this.replication = replication;
    }

    private void manageHeartBeats(List<Long> heartbeatList) {
        heartbeatsManager.updateHeartbeats(heartbeatList);
        List<Node> recoveredNodes = heartbeatsManager.updateNodesStatus();

        for (Node node: recoveredNodes) {
            Set<Node> successorNodes = nodesCircle.findSuccessorNodes(node);
            keyTransferManager.sendMessageToSuccessor(successorNodes, node);
        }
    }

    @Override
    public void run() {
        try {
            // Request payload from client
            KeyValueRequest.KVRequest reqPayload = KeyValueRequest.KVRequest
                    .parseFrom(requestMessage.getPayload().toByteArray());

            int command = reqPayload.getCommand();

            // Receive heartbeats
            if (command == Command.HEARTBEAT.getCode()) {
                manageHeartBeats(reqPayload.getHeartbeatList());
                return;
            }

            // Receive keys transfer
            if  (command == Command.KEY_TRANSFER.getCode()) {
                addKey(reqPayload.getPair());
                return;
            }

            // Receive notify to transfer keys to recovered node as its successor
            if  (command == Command.SUCCESSOR_NOTIFY.getCode()) {
                Node node = nodesCircle.getNodeById(reqPayload.getRecoveredNodeId());
                List<KeyValueRequest.HashRange> hashRanges = reqPayload.getHashRangesList();
                List<ByteString> keysToRemove = new ArrayList<>();
                keysToRemove.addAll(keyTransferManager.transferKeysWithinRange(node, hashRanges));
                if (!keysToRemove.isEmpty()) {
                    for (ByteString key: keysToRemove) {
                        store.getStore().remove(key);
                    }
                }
//                System.out.println(KVServer.port + " is a successor of " + node.getPort());
                return;
            }

            // reroute PUT/GET/REMOVE requests if come directly from client and don't belong to current node
            if (command <= 3 && command >= 1 && !requestMessage.hasClientAddress()) {
                // Find correct node and Reroute
//                byte[] key = reqPayload.getKey().toByteArray();
//                String sha256 = Hashing.sha256()
//                        .hashBytes(key).toString();

                heartbeatsManager.removeDeadNodes();
//                Node node = nodesCircle.findCorrectNodeByHash(sha256.hashCode());

//                if (node.getId() != nodesCircle.getThisNodeId()) {
//                    reRoute(node);
//                    return;
//                }

                ByteString key = reqPayload.getKey();
//                if((command == Command.PUT.getCode() || command == Command.REMOVE.getCode()) && (!replication.isPrimary(key))) {
//                    reRoute(nodesCircle.findNodebyKey(key));
//                    return;
//                }

                if(!replication.isPrimary(key)) {
                    reRoute(nodesCircle.findNodebyKey(key));
                    return;
                }
            }

            if (command == Command.BACKUP_WRITE.getCode()) {
                backupPUT(reqPayload);
                return;
            }

            if (command == Command.BACKUP_REM.getCode()) {
                backupREM(reqPayload);
                return;
            }

            if (command == Command.BACKUP_ACK.getCode()) {
                handleBackupAck();
                return;
            }

            // 1. request comes from another node who thinks I'm the right node
            // 2. request from client, but I think im the right node
            // could be true or im temporarily storing the data b/c the actual right node is down
            getResponseFromOwnNode(reqPayload);
        } catch (IOException e) {
            System.out.println("===================");
            System.out.println("[ KV Server Handler, "+socket.getLocalPort()+", " + Thread.currentThread().getName() + "]: "
                    + e.getLocalizedMessage() + e.getMessage());
            System.out.println("===================");
            throw new RuntimeException(e);
        }
    }

    private void handleBackupAck() {
        replication.updateWriteAckCache(requestMessage.getMessageID());
    }

    private void backupPUT(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        if (isMemoryOverload()) {
            // To client
            replication.sendResponseToClient(InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()),
                    requestMessage.getClientPort(), requestMessage.getMessageID(), ErrorCode.OUT_OF_SPACE);
            return;
        }
        Value valueV = new Value(requestPayload.getVersion(), requestPayload.getValue());
        store.getStore().put(requestPayload.getKey(), valueV);
        //System.out.println(socket.getLocalPort() + " save: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));

        // To primary
        replication.sendAckToPrimary(address, port, requestMessage.getMessageID());
    }

    private void backupREM(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        if (store.getStore().get(requestPayload.getKey()) == null) {
            return;
        }
        store.getStore().remove(requestPayload.getKey());

        // To primary
        replication.sendAckToPrimary(address, port, requestMessage.getMessageID());
    }

    private void addKey(KeyValueRequest.KeyValueEntry pair) {
        //System.out.println(KVServer.port + " received key transfer: " + StringUtils.byteArrayToHexString(pair.getKey().toByteArray()));
        store.getStore().put(
                pair.getKey(),
                new Value(pair.getVersion(), pair.getValue()));

//        byte[] key = pair.getKey().toByteArray();
//        String sha256 = Hashing.sha256()
//                .hashBytes(key).toString();
//        Node nodeMatch = nodesCircle.findCorrectNodeByHash(sha256.hashCode());
//
//        if(nodesCircle.getAliveNodesList().containsValue(nodeMatch) && nodeMatch.getId() != nodesCircle.getThisNodeId())
//        {
//            //System.out.println("send to " + nodeMatch.getPort());
//            List<KeyValueRequest.KeyValueEntry> allPairs = new ArrayList<>();
//            allPairs.add(pair);
//            KeyTransferManager.getInstance().sendMessage(allPairs, nodeMatch);
//        }

    }

    private void getResponseFromOwnNode(KeyValueRequest.KVRequest reqPayload) throws IOException {
        // If cached request, get response msg from cache and send it
        byte[] id = requestMessage.getMessageID().toByteArray();
        Message.Msg cachedResponse = storeCache.getCache().getIfPresent(ByteBuffer.wrap(id));
        if (cachedResponse != null) {
            sendResponse(cachedResponse, requestMessage);
            return;
        }

        // Prepare response payload as per client's command
        KeyValueResponse.KVResponse responsePayload = processRequest(reqPayload);

        // Attach payload, id, and checksum to reply message
        CRC32 checksum = new CRC32();
        checksum.update(id);
        checksum.update(responsePayload.toByteArray());

        Message.Msg responseMsg = Message.Msg.newBuilder()
                .setMessageID(requestMessage.getMessageID())
                .setPayload(responsePayload.toByteString())
                .setCheckSum(checksum.getValue())
                .build();
        sendResponse(responseMsg, requestMessage);
        storeCache.getCache().put(ByteBuffer.wrap(id), responseMsg);
    }

    private void reRoute(Node node) throws IOException {
        Message.Msg.Builder msgBuilder = Message.Msg.newBuilder()
                    .setMessageID(requestMessage.getMessageID())
                    .setPayload(requestMessage.getPayload())
                    .setCheckSum(requestMessage.getCheckSum())
                    .setClientPort(this.port)
                    .setClientAddress(ByteString.copyFrom(this.address.getAddress()));

        byte[] responseAsByteArray = msgBuilder.build().toByteArray();
        DatagramPacket responsePkt = new DatagramPacket(
                responseAsByteArray,
                responseAsByteArray.length,
                node.getAddress(),
                node.getPort());
        socket.send(responsePkt);
    }

    private void sendResponse(Message.Msg msg, Message.Msg requestMessage) throws IOException {
        byte[] responseAsByteArray = msg.toByteArray();
        DatagramPacket responsePkt;
        if (requestMessage.hasClientAddress()) {
            responsePkt = new DatagramPacket(
                    responseAsByteArray,
                    responseAsByteArray.length,
                    InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()),
                    requestMessage.getClientPort());
        } else {
            responsePkt = new DatagramPacket(
                    responseAsByteArray,
                    responseAsByteArray.length,
                    this.address,
                    this.port);
        }
        socket.send(responsePkt);
    }

    /*
     *  Generate a response payload
     */
    private KeyValueResponse.KVResponse processRequest(KeyValueRequest.KVRequest requestPayload) throws UnknownHostException {
        // Get command, key, and value from request
        int commandCode = requestPayload.getCommand();
        ByteString key = requestPayload.getKey();
        byte[] value = requestPayload.getValue().toByteArray();

        // Find corresponding Command
        Optional<Command> command = Command.findCommand(commandCode);
        KeyValueResponse.KVResponse.Builder builder = KeyValueResponse.KVResponse.newBuilder();

        if (command.isEmpty()) {
            return builder
                    .setErrCode(ErrorCode.UNKNOWN_COMMAND.getCode()).build();
        }

        if (key.size() > KVServer.MAX_KEY_LENGTH) {
            return builder
                    .setErrCode(ErrorCode.INVALID_KEY.getCode())
                    .build();
        }
        if (value.length > KVServer.MAX_VALUE_LENGTH) {
            return builder
                    .setErrCode(ErrorCode.INVALID_VALUE.getCode())
                    .build();
        }

        Boolean remove = false;

        switch (command.get()) {
            case PUT:
                if (isMemoryOverload()) {
                    return builder
                            .setErrCode(ErrorCode.OUT_OF_SPACE.getCode())
                            .build();
                }
                replication.createWriteAckCache(requestMessage.getMessageID(),
                        requestMessage.hasClientAddress()? InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()): address,
                        requestMessage.hasClientPort()? requestMessage.getClientPort(): port);

                Value valueV = new Value(requestPayload.getVersion(), requestPayload.getValue());
                store.getStore().put(key, valueV);
                //System.out.println(socket.getLocalPort() + " save: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));

                replication.sendWriteToBackups(requestPayload, requestMessage.getMessageID(), remove);

                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case GET:
                    Value valueInStore = store.getStore().get(key);
                    if (valueInStore == null) {
                       //System.out.println(socket.getLocalPort() + " no key: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));
                        return builder
                                .setErrCode(ErrorCode.NONEXISTENT_KEY.getCode())
                                .build();
                    }

                    return builder
                            .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                            .setValue(valueInStore.getValue())
                            .setVersion(valueInStore.getVersion())
                            .build();
            case REMOVE:
                if (store.getStore().get(key) == null) {
                    return builder
                            .setErrCode(ErrorCode.NONEXISTENT_KEY.getCode())
                            .build();
                }

                replication.createWriteAckCache(requestMessage.getMessageID(),
                        requestMessage.hasClientAddress()? InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()): address,
                        requestMessage.hasClientPort()? requestMessage.getClientPort(): port);

                store.getStore().remove(key);

                remove = true;
                replication.sendWriteToBackups(requestPayload, requestMessage.getMessageID(), remove);

                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case SHUTDOWN:
                System.exit(0);
            case WIPE_OUT:
                wipeOut();
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case IS_ALIVE:
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case GET_PID:
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .setPid(KVServer.PROCESS_ID)
                        .build();
            case GET_MEMBERSHIP_COUNT:
                heartbeatsManager.removeDeadNodes();
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .setMembershipCount(nodesCircle.getAliveNodesCount())
                        .build();
        }
        return builder
                .setErrCode(ErrorCode.INTERNAL_FAILURE.getCode())
                .build();
    }

    private  void wipeOut() {
        store.clearStore();
        storeCache.clearCache();
        Runtime.getRuntime().freeMemory();
        System.gc();
    }

    private boolean isMemoryOverload() {
        return MemoryUsage.getFreeMemory() < 0.04 * MemoryUsage.getMaxMemory();
    }
}


