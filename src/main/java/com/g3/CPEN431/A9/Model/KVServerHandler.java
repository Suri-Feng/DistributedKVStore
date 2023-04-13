package com.g3.CPEN431.A9.Model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A9.Model.Distribution.*;
import com.g3.CPEN431.A9.Utility.MemoryUsage;
import com.g3.CPEN431.A9.Model.Store.KVStore;
import com.g3.CPEN431.A9.Model.Store.StoreCache;
import com.g3.CPEN431.A9.Model.Store.Value;
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
    NodeCircleManager nodeCircleManager = NodeCircleManager.getInstance();
    HeartbeatsManager heartbeatsManager = HeartbeatsManager.getInstance();
    KeyTransferManager keyTransferManager = KeyTransferManager.getInstance();

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

            if(nodesCircle.getStartupNodesSize() == 1) {
                getResponseFromOwnNode(reqPayload);
                return;
            }

            int command = reqPayload.getCommand();

            // Receive heartbeats
            if (command == Command.HEARTBEAT.getCode()) {
//                if(KVServer.port == 12385 ) {
//                    System.out.println("12385 recv heartbeat from " + port + ", before update" +  heartbeatsManager.getHeartBeats().get(port - 12385) + ", "+ (System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(port - 12385)));
//                }
                List<Long> heartbeats = reqPayload.getHeartbeatList();

                manageHeartBeats(heartbeats);

                if((System.currentTimeMillis() - heartbeats.get(nodesCircle.getThisNodeId())) > heartbeatsManager.mostPastTime) {
//                    System.out.println(KVServer.port + " will not update node circle");
                    return;
                }
                nodeCircleManager.updateNodeCircle();
//                if(KVServer.port == 12385 ) {
//                    System.out.println("12385 recv heartbeat from " + port + ", after update" + heartbeatsManager.getHeartBeats().get(port - 12385) + ", "+(System.currentTimeMillis() - heartbeatsManager.getHeartBeats().get(port - 12385)));
//                }
                return;
            }

            if (command >= 21 && command <= 27) {
                keyTransferManager.handleReplicationRequest(reqPayload);
                return;
            }


            // reroute PUT/GET/REMOVE requests if come directly from client and don't belong to current node
            if (command <= 3 && command >= 1 && !requestMessage.hasClientAddress()) {
                ByteString key = reqPayload.getKey();

                if(!isPrimary(key)) {
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
            System.out.println("[ KV Server Handler, "+socket.getLocalPort()+", " + Thread.currentThread().getName() + "]: "
                    + e.getLocalizedMessage() + e.getMessage());
            System.out.println("===================");
            throw new RuntimeException(e);
        }
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

        switch (command.get()) {
            case PUT:
                if (isMemoryOverload()) {
                    return builder
                            .setErrCode(ErrorCode.OUT_OF_SPACE.getCode())
                            .build();
                }

                Value valueV = new Value(requestPayload.getVersion(), requestPayload.getValue());
                store.getStore().put(key, valueV);
//                System.out.println(socket.getLocalPort() + " save: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));

                if(nodesCircle.getStartupNodesSize() != 1)
                    keyTransferManager.sendPUTtoBackups(requestPayload, requestMessage.getMessageID());

                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case GET:
                    Value valueInStore = store.getStore().get(key);
                    if (valueInStore == null) {
//                       System.out.println(socket.getLocalPort() + " no key: " + StringUtils.byteArrayToHexString(requestPayload.getKey().toByteArray()));
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

                if(nodesCircle.getStartupNodesSize() != 1)
                    keyTransferManager.sendREMtoBackups(requestPayload, requestMessage.getMessageID());

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
                if(nodesCircle.getStartupNodesSize() == 1)
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
        return MemoryUsage.getFreeMemory() < 0.04 * MemoryUsage.getMaxMemory();
    }


    public boolean isPrimary(ByteString key) {
        NodesCircle nodesCircle = NodesCircle.getInstance();
        return nodesCircle.findNodebyKey(key).getId() == nodesCircle.getThisNodeId();
    }
}


