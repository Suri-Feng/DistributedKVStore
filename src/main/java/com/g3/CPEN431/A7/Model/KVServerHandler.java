package com.g3.CPEN431.A7.Model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A7.Utility.MemoryUsage;
import com.g3.CPEN431.A7.Model.Distribution.HeartbeatsManager;
import com.g3.CPEN431.A7.Model.Distribution.Node;
import com.g3.CPEN431.A7.Model.Distribution.NodesCircle;
import com.g3.CPEN431.A7.Model.Store.KVStore;
import com.g3.CPEN431.A7.Model.Store.StoreCache;
import com.g3.CPEN431.A7.Model.Store.ValueV;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;

public class KVServerHandler implements Runnable {
    DatagramSocket socket;
    Message.Msg requestMessage;
    InetAddress address;
    int port;
    KVStore store = KVStore.getInstance();
    StoreCache storeCache = StoreCache.getInstance();
    NodesCircle nodesCircle = NodesCircle.getInstance();
    HeartbeatsManager heartbeatsManager = HeartbeatsManager.getInstance();

    KVServerHandler(Message.Msg requestMessage,
                    DatagramSocket socket,
                    InetAddress address,
                    int port) {
        this.socket = socket;
        this.requestMessage = requestMessage;
        this.address = address;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            // Request payload from client
            KeyValueRequest.KVRequest reqPayload = KeyValueRequest.KVRequest
                    .parseFrom(requestMessage.getPayload().toByteArray());

            int command = reqPayload.getCommand();

            if (command == Command.HEARTBEAT.getCode()) {
                heartbeatsManager.updateHeartbeats(reqPayload.getHeartbeatList());
                return;
            }

            heartbeatsManager.recoverLiveNodes();
            // Only reroute PUT/GET/REMOVE requests
            if (command <= 3 && command >= 1 && !requestMessage.hasClientAddress()) {
                int correctNodeRingHash = -1;
                Node node = null;
                // Find correct node and Reroute
                do {
                    if (correctNodeRingHash != -1) {
                        nodesCircle.removeNode(correctNodeRingHash);
                        System.out.println("Dead node: " + node.getPort() + " Num servers left: " + nodesCircle.getAliveNodesCount());
                        System.out.println("I am node " + nodesCircle.getThisNodeId() + " , I have " + store.getStore().size() + " keys");
                    }
                    correctNodeRingHash = nodesCircle.findRingKeyByHash(reqPayload.getKey().hashCode());
                    node = nodesCircle.getCircle().get(correctNodeRingHash);
                } while (!heartbeatsManager.isNodeAlive(node.getId()));

                if (node.getId() != nodesCircle.getThisNodeId()) {
                    reRoute(node);
                    return;
                }
            }

            getResponseFromOwnNode(reqPayload);
        } catch (IOException e) {
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
        Message.Msg msg = Message.Msg.newBuilder()
                .setMessageID(requestMessage.getMessageID())
                .setPayload(requestMessage.getPayload())
                .setCheckSum(requestMessage.getCheckSum())
                .setClientPort(this.port)
                .setClientAddress(ByteString.copyFrom(this.address.getAddress()))
                .build();
        byte[] responseAsByteArray = msg.toByteArray();
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
//            System.out.println("==============");
//            System.out.println("ID: "
//                    + StringUtils.byteArrayToHexString(requestMessage.getMessageID().toByteArray() ));
////            System.out.println("rerouted from: " + port  + ". Now Sending to  " + requestMessage.getClientPort());
//            System.out.println("==============");
            responsePkt = new DatagramPacket(
                    responseAsByteArray,
                    responseAsByteArray.length,
                    InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()),
                    requestMessage.getClientPort());
        } else {
//            System.out.println("==============");
//            System.out.println("From client: " + this.port + "。 Sending to port: " + this.port);
//            System.out.println("==============");
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

        byte[] key = requestPayload.getKey().toByteArray();
        byte[] value = requestPayload.getValue().toByteArray();

        // Find corresponding Command
        Optional<Command> command = Command.findCommand(commandCode);
        KeyValueResponse.KVResponse.Builder builder = KeyValueResponse.KVResponse.newBuilder();

        if (command.isEmpty()) {
            return builder
                    .setErrCode(ErrorCode.UNKNOWN_COMMAND.getCode()).build();
        }

        if (key.length > KVServer.MAX_KEY_LENGTH) {
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

                ValueV valueV = new ValueV(requestPayload.getVersion(), requestPayload.getValue());
                store.getStore().put(ByteBuffer.wrap(key),
                        valueV);

                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case GET:
                ValueV valueInStore = store.getStore().get(ByteBuffer.wrap(key));
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
                if (store.getStore().get(ByteBuffer.wrap(key)) == null) {
                    return builder
                            .setErrCode(ErrorCode.NONEXISTENT_KEY.getCode())
                            .build();
                }
                store.getStore().remove(ByteBuffer.wrap(key));
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case SHUTDOWN:
                System.out.println("Before shutting down. I am node " + nodesCircle.getThisNodeId() + " , I have " + store.getStore().size() + " keys");
//                System.exit(0);
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
        return MemoryUsage.getFreeMemory() < 0.035 * MemoryUsage.getMaxMemory();
    }
}


