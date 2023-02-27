package com.s42442146.CPEN431.A4.model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.s42442146.CPEN431.A4.Utility.MemoryUsage;
import com.s42442146.CPEN431.A4.model.Distribution.HeartbeatsManager;
import com.s42442146.CPEN431.A4.model.Distribution.Node;
import com.s42442146.CPEN431.A4.model.Distribution.NodesCircle;
import com.s42442146.CPEN431.A4.model.Store.KVStore;
import com.s42442146.CPEN431.A4.model.Store.StoreCache;
import com.s42442146.CPEN431.A4.model.Store.ValueV;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;

import static com.s42442146.CPEN431.A4.model.Command.*;
import static com.s42442146.CPEN431.A4.model.ErrorCode.*;

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
            byte[] id = requestMessage.getMessageID().toByteArray();
            // Request payload from client
            KeyValueRequest.KVRequest reqPayload = KeyValueRequest.KVRequest
                    .parseFrom(requestMessage.getPayload().toByteArray());

            int command = reqPayload.getCommand();

            if (command == 10) {
                System.out.println("=====================");
                heartbeatsManager.updateHeartbeats(reqPayload.getHeartbeatList());
                return;
            }
            if (command <= 3 && command >= 1 && !requestMessage.hasClientAddress()) {
                int bucketHash = nodesCircle.findRingKeyByHash(reqPayload.getKey().hashCode());
                // Reroute
                if (bucketHash != nodesCircle.getThisNodeHash()) {
                    Node node = nodesCircle.getCircle().get(bucketHash);
//                    System.out.println("==============");
//                    System.out.println("Rerouting to port: " + node.getPort() + " Command: " + reqPayload.getCommand() + " ID: " + StringUtils.byteArrayToHexString(requestMessage.getMessageID().toByteArray() )
//                    + " setting Client port: " + this.port);
//                    System.out.println("==============");
                    reRoute(bucketHash);
                    return;
                }
            }

            // If cached request, get response msg from cache and send it
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void reRoute(int nodeHash) throws IOException {
        Node node = nodesCircle.getCircle().get(nodeHash);
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
//            System.out.println("rerouted from: " + port + " id: " + requestMessage.getMessageID() + ". Now Sending to  " + requestMessage.getClientPort());
//            System.out.println("==============");
            responsePkt = new DatagramPacket(
                    responseAsByteArray,
                    responseAsByteArray.length,
                    InetAddress.getByAddress(requestMessage.getClientAddress().toByteArray()),
                    requestMessage.getClientPort());
        } else {
//            System.out.println("==============");
//            System.out.println("From client: " + this.port + "。 Sending to port: " + this.port + " id" + requestMessage.getMessageID());
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
        Optional<Command> command = findCommand(commandCode);
        KeyValueResponse.KVResponse.Builder builder = KeyValueResponse.KVResponse.newBuilder();

        if (command.isEmpty()) {
            return builder
                    .setErrCode(UNKNOWN_COMMAND.getCode()).build();
        }

        if (key.length > KVServer.MAX_KEY_LENGTH) {
            return builder
                    .setErrCode(INVALID_KEY.getCode())
                    .build();
        }
        if (value.length > KVServer.MAX_VALUE_LENGTH) {
            return builder
                    .setErrCode(INVALID_VALUE.getCode())
                    .build();
        }

        switch (command.get()) {
            case PUT:
                if (isMemoryOverload()) {
                    return builder
                            .setErrCode(OUT_OF_SPACE.getCode())
                            .build();
                }

                ValueV valueV = new ValueV(requestPayload.getVersion(), requestPayload.getValue());
                store.getStore().put(ByteBuffer.wrap(key),
                        valueV);

                return builder
                        .setErrCode(SUCCESSFUL.getCode())
                        .build();
            case GET:
                ValueV valueInStore = store.getStore().get(ByteBuffer.wrap(key));
                if (valueInStore == null) {
                    return builder
                            .setErrCode(NONEXISTENT_KEY.getCode())
                            .build();
                }
                return builder
                        .setErrCode(SUCCESSFUL.getCode())
                        .setValue(valueInStore.getValue())
                        .setVersion(valueInStore.getVersion())
                        .build();
            case REMOVE:
                if (store.getStore().get(ByteBuffer.wrap(key)) == null) {
                    return builder
                            .setErrCode(NONEXISTENT_KEY.getCode())
                            .build();
                }
                store.getStore().remove(ByteBuffer.wrap(key));
                return builder
                        .setErrCode(SUCCESSFUL.getCode())
                        .build();
            case SHUTDOWN:
                System.exit(0);
            case WIPE_OUT:
                wipeOut();
                return builder
                        .setErrCode(SUCCESSFUL.getCode())
                        .build();
            case IS_ALIVE:
                return builder
                        .setErrCode(SUCCESSFUL.getCode())
                        .build();
            case GET_PID:
                return builder
                        .setErrCode(SUCCESSFUL.getCode())
                        .setPid((int) ProcessHandle.current().pid())
                        .build();
            case GET_MEMBERSHIP_COUNT:
                return builder
                        .setErrCode(SUCCESSFUL.getCode())
                        .setMembershipCount(1)
                        .build();
        }
        return builder
                .setErrCode(INTERNAL_FAILURE.getCode())
                .build();
    }

    private  void wipeOut() {
        store.clearStore();
        storeCache.clearCache();
        Runtime.getRuntime().freeMemory();
        System.gc();
    }

    private boolean verifyChecksum(Message.Msg request) {
        CRC32 check = new CRC32();
        check.update(request.getMessageID().toByteArray());
        check.update(request.getPayload().toByteArray());

        return check.getValue() == request.getCheckSum();
    }

    private boolean isMemoryOverload() {
        return MemoryUsage.getFreeMemory() < 0.035 * MemoryUsage.getMaxMemory();
    }
}


