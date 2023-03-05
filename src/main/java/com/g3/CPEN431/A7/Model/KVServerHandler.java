package com.g3.CPEN431.A7.Model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.A7.Model.Store.HintedStore;
import com.g3.CPEN431.A7.Utility.MemoryUsage;
import com.g3.CPEN431.A7.Model.Distribution.HeartbeatsManager;
import com.g3.CPEN431.A7.Model.Distribution.Node;
import com.g3.CPEN431.A7.Model.Distribution.NodesCircle;
import com.g3.CPEN431.A7.Model.Store.KVStore;
import com.g3.CPEN431.A7.Model.Store.StoreCache;
import com.g3.CPEN431.A7.Model.Store.ValueV;
import com.g3.CPEN431.A7.Utility.StringUtils;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    HintedStore hintedStore = HintedStore.getInstance();

    Node hintedNode;

    KVServerHandler(Message.Msg requestMessage,
                    DatagramSocket socket,
                    InetAddress address,
                    int port) {
        this.socket = socket;
        this.requestMessage = requestMessage;
        this.address = address;
        this.port = port;
        hintedNode = null;
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
                heartbeatsManager.recoverLiveNodes();
                // TODO: send data to recovered nodes
                return;
            }

            // reroute PUT/GET/REMOVE requests if come directly from client and don't belong to current node
            if (command <= 3 && command >= 1 && !requestMessage.hasClientAddress()) {
                Node node = null;
                // Find correct node and Reroute
                byte[] key = reqPayload.getKey().toByteArray();
                String sha256 = Hashing.sha256()
                        .hashBytes(key).toString();
                do {
                    if (node != null) {
                        System.out.println(socket.getLocalPort() + ": remove node: " + node.getPort() + " is alive: " + heartbeatsManager.isNodeAlive(node));
                        nodesCircle.removeNode(node);
                        // hintedNode is what the request should've been sent to if not null.
                        // Instead, request will be sent to node.
                        hintedNode = node;
                    }
                    node = nodesCircle.findCorrectNodeByHash(sha256.hashCode());
                } while (!heartbeatsManager.isNodeAlive(node));

                if (node.getId() != nodesCircle.getThisNodeId()) {
                    reRoute(node);
                    return;
                }
            }

            // 1. request comes from another node
            // 2. request from client but belongs to my node
            // could be im the right node or im temporarily storing the data b/c hintedNode is not null



            // For GET/REMOVE requests, if I don't have the key, then forward to next node in the ring
//            if (command == Command.GET.getCode() || command == Command.REMOVE.getCode()) {
//                byte[] id = requestMessage.getMessageID().toByteArray();
//                if (!store.getStore().containsKey(ByteBuffer.wrap(id))) {
//                    // Reroute to next node
//                    Node nextNode = nodesCircle.findNextNode(correctNodeRingHash);
//                    reRoute(nextNode);
//                    return;
//                }
//            }
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
        Message.Msg.Builder msgBuilder = Message.Msg.newBuilder()
                    .setMessageID(requestMessage.getMessageID())
                    .setPayload(requestMessage.getPayload())
                    .setCheckSum(requestMessage.getCheckSum())
                    .setClientPort(this.port)
                    .setClientAddress(ByteString.copyFrom(this.address.getAddress()));

        if (hintedNode != null) {
            msgBuilder.setHintedNodeId(hintedNode.getId());
        }

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
                if (hintedNode == null) {
                    store.getStore().put(ByteBuffer.wrap(key), valueV);
                } else {
                    ConcurrentHashMap<ByteBuffer, ValueV> tempStore = hintedStore.getStore().get(hintedNode.getId());
                    if (tempStore == null) {
                        tempStore = new ConcurrentHashMap<>();
                        tempStore.put(ByteBuffer.wrap(key), valueV);
                        hintedStore.getStore().put(hintedNode.getId(), tempStore);
                    } else {
                        tempStore.put(ByteBuffer.wrap(key), valueV);
                    }
                }
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .build();
            case GET:
                if (hintedStore == null) {

                }
                ValueV valueInStore = store.getStore().get(ByteBuffer.wrap(key));
                if (valueInStore == null) {
                    return builder
                            .setErrCode(ErrorCode.NONEXISTENT_KEY.getCode())
                            .build();
                }
//                System.out.println(socket.getLocalPort()  + " number of keys: "
//                        + store.getStore().size());
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
                System.out.println(socket.getLocalPort() + " has " + store.getStore().size() + " keys");
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
                return builder
                        .setErrCode(ErrorCode.SUCCESSFUL.getCode())
                        .setMembershipCount(1)
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
        return MemoryUsage.getFreeMemory() < 0.1 * MemoryUsage.getMaxMemory();
    }
}


