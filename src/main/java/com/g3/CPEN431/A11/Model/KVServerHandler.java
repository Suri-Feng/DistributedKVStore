package com.g3.CPEN431.A11.Model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A11.Model.Distribution.*;
import com.g3.CPEN431.A11.Model.Store.QueuedMessage;
import com.g3.CPEN431.A11.Utility.MemoryUsage;
import com.g3.CPEN431.A11.Model.Store.KVStore;
import com.g3.CPEN431.A11.Model.Store.StoreCache;
import com.g3.CPEN431.A11.Model.Store.Value;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;
import com.g3.CPEN431.A11.Model.Distribution.KeyTransferManager;

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
    NodesCircleManager nodesCircleManager = NodesCircleManager.getInstance();
    KVServerHandler(Message.Msg requestMessage,
                    DatagramSocket socket,
                    InetAddress address,
                    int port
    ) {
        this.socket = socket;
        this.requestMessage = requestMessage;
        this.address = address;
        this.port = port;
    }

    private void manageHeartBeats(List<Long> heartbeatList) {
        heartbeatsManager.updateHeartbeats(heartbeatList);
    }

    @Override
    public void run() {
        try {
            // Request payload from client
            KeyValueRequest.KVRequest reqPayload = KeyValueRequest.KVRequest
                    .parseFrom(requestMessage.getPayload().toByteArray());

            if (nodesCircle.getStartupNodesSize() == 1) {
                getResponseFromOwnNode(reqPayload);
                return;
            }

            int command = reqPayload.getCommand();

//            if (command == Command.PUT_ACK.getCode()) {
//                processBackupAck();
//                return;
//            }

            // Receive heartbeats
            if (command == Command.HEARTBEAT.getCode()) {
                List<Long> heartbeats = reqPayload.getHeartbeatList();
                manageHeartBeats(heartbeats);
                if ((System.currentTimeMillis() - heartbeats.get(nodesCircle.getThisNodeId())) > heartbeatsManager.mostPastTime) {
                    return;
                }
                nodesCircleManager.updateNodeCircle();
                return;
            }

            if (command >= 21 && command <= 28) {
                keyTransferManager.handleReplicationRequest(reqPayload);
                return;
            }

            // reroute PUT/GET/REMOVE requests if come directly from client and don't belong to current node
            if (command <= 3 && command >= 1 && !requestMessage.hasClientAddress()) {
                ByteString key = reqPayload.getKey();

                if (!isPrimary(key)) {
                    reRoute(nodesCircle.findNodebyKey(key));
                    return;
                }
            }


            // 1. request comes from another node who thinks I'm the right node
            // 2. request from client, but I think im the right node
            // could be true or im temporarily storing the data b/c the actual right node is down
            getResponseFromOwnNode(reqPayload);
        } catch (IOException e) {
            System.out.println("===================");
            System.out.println("[ KV Server Handler, " + socket.getLocalPort() + ", " + Thread.currentThread().getName() + "]: "
                    + e.getLocalizedMessage() + e.getMessage());
            System.out.println("===================");
            throw new RuntimeException(e);
        }
    }

    private void processBackupAck() throws IOException {
        QueuedMessage queuedMessage = storeCache.getQueuedResponses().getIfPresent(requestMessage.getMessageID());

        if (queuedMessage != null) {
            byte[] id = queuedMessage.getId().toByteArray();

            queuedMessage.addAckPort(this.port);

            if (queuedMessage.getAckPortsCount() == 3) {
                KeyValueResponse.KVResponse responsePayload = KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
                CRC32 checksum = new CRC32();

                checksum.update(id);
                checksum.update(responsePayload.toByteArray());

                Message.Msg responseMsg = Message.Msg.newBuilder()
                        .setMessageID(queuedMessage.getId())
                        .setPayload(responsePayload.toByteString())
                        .setCheckSum(checksum.getValue())
                        .build();

                if (storeCache.getCache().getIfPresent(ByteString.copyFrom(id)) == null) {
                    storeCache.getCache().put(ByteString.copyFrom(id), responseMsg);
                    storeCache.getQueuedResponses().invalidate(requestMessage.getMessageID());
                    sendResponse(responseMsg, queuedMessage.getAddress(), queuedMessage.getPort());
                }
            }
        }
    }

    private void getResponseFromOwnNode(KeyValueRequest.KVRequest reqPayload) throws IOException {
        // If cached request, get response msg from cache and send it
        byte[] id = requestMessage.getMessageID().toByteArray();
        Message.Msg cachedResponse = storeCache.getCache().getIfPresent(ByteString.copyFrom(id));
        if (cachedResponse != null) {
            sendResponse(cachedResponse, requestMessage);
            return;
        }

        // Prepare response payload as per client's command
        KeyValueResponse.KVResponse responsePayload = processRequest(reqPayload);

//        if (nodesCircle.getStartupNodesSize() != 1 && reqPayload.getCommand() == Command.PUT.getCode()
//                && responsePayload.getErrCode() == ErrorCode.SUCCESSFUL.getCode()) {
//            String uuid = UUID.randomUUID().toString();
//            QueuedMessage queuedMessage;
//            if (!requestMessage.hasClientPort()) {
//                queuedMessage = new QueuedMessage(this.address, this.port, requestMessage.getMessageID(), reqPayload.getKey(), reqPayload.getValue(), reqPayload.getVersion());
//            } else {
//                queuedMessage = new QueuedMessage(InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()),
//                        requestMessage.getClientPort(),
//                        requestMessage.getMessageID(),
//                        reqPayload.getKey(),
//                        reqPayload.getValue(),
//                        reqPayload.getVersion());
//            }
//            long R_TS = System.currentTimeMillis();
//            putValueInStore(reqPayload.getKey(), reqPayload.getValue(), reqPayload.getVersion(), R_TS);
//            storeCache.getQueuedResponses().put(ByteString.copyFromUtf8(uuid), queuedMessage);
//            sendWriteToBackups(reqPayload, ByteString.copyFromUtf8(uuid), false, R_TS);
//            return;
//        }
        // Attach payload, id, and checksum to reply message
//        long R_TS = System.currentTimeMillis();
//        boolean sendWriteToBackups = false;
//        if (reqPayload.getCommand() == Command.PUT.getCode() && responsePayload.getErrCode() == ErrorCode.SUCCESSFUL.getCode()) {
//            if (nodesCircle.getStartupNodesSize() == 1) {
//                store.getStore().put(reqPayload.getKey(), new Value(reqPayload.getVersion(), reqPayload.getValue()));
//            } else {
//                sendWriteToBackups = true;
//                putValueInStore(reqPayload.getKey(), reqPayload.getValue(), reqPayload.getVersion(), R_TS);
//            }
//        }

        CRC32 checksum = new CRC32();
        checksum.update(id);
        checksum.update(responsePayload.toByteArray());

        Message.Msg responseMsg = Message.Msg.newBuilder()
                .setMessageID(requestMessage.getMessageID())
                .setPayload(responsePayload.toByteString())
                .setCheckSum(checksum.getValue())
                .build();

        sendResponse(responseMsg, requestMessage);
        storeCache.getCache().put(ByteString.copyFrom(id), responseMsg);
//
//        if (nodesCircle.getStartupNodesSize() != 1 &&
//                reqPayload.getCommand() == Command.REMOVE.getCode() && responsePayload.getErrCode() == ErrorCode.SUCCESSFUL.getCode()) {
//            keyTransferManager.sendREMtoBackups(reqPayload, requestMessage.getMessageID(), 0);
//        }
//        if (sendWriteToBackups) {
//            keyTransferManager.sendPUTtoBackups(reqPayload, requestMessage.getMessageID(), R_TS);
//        }
    }

    private void sendResponse(Message.Msg msg, InetAddress address, int port) throws IOException {
        byte[] responseAsByteArray = msg.toByteArray();
        DatagramPacket responsePkt = new DatagramPacket(
                    responseAsByteArray,
                    responseAsByteArray.length,
                    address,
                    port);
        socket.send(responsePkt);
    }

    private void putValueInStore(ByteString key, ByteString value, int version, long R_TS) {
        store.getStore().put(key, new Value(version, value, R_TS));
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
    private KeyValueResponse.KVResponse processRequest(KeyValueRequest.KVRequest requestPayload) {
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

        switch (command.get()) {
            case PUT:
                if (isMemoryOverload()) {
                    return builder
                            .setErrCode(ErrorCode.OUT_OF_SPACE.getCode())
                            .build();
                }
//                if (nodesCircle.getStartupNodesSize() == 1) {
//                    store.getStore().put(requestPayload.getKey(), new Value(requestPayload.getVersion(), requestPayload.getValue()));
//                }
                long R_TS = System.currentTimeMillis();
                putValueInStore(requestPayload.getKey(), requestPayload.getValue(), requestPayload.getVersion(), R_TS);
                keyTransferManager.sendPUTtoBackups(requestPayload, requestMessage.getMessageID(), R_TS);
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case GET:
                Value valueInStore = store.getStore().get(key);
                if (valueInStore == null) {
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

                store.getStore().remove(key);
                keyTransferManager.sendREMtoBackups(requestPayload, requestMessage.getMessageID(), 0);
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
                if (nodesCircle.getStartupNodesSize() == 1)
                    return builder
                            .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                            .setMembershipCount(1)
                            .build();
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .setMembershipCount(nodesCircle.getAliveNodesCount())
                        .build();
        }
        return builder
                .setErrCode(ErrorCode.INTERNAL_FAILURE.getCode())
                .build();
    }

    private void wipeOut() {
        store.clearStore();
        storeCache.clearCache();
        Runtime.getRuntime().freeMemory();
        System.gc();
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
}


