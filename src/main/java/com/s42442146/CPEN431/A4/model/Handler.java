package com.s42442146.CPEN431.A4.model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import ca.NetSysLab.ProtocolBuffers.Value;
import com.google.common.cache.Cache;
import com.s42442146.CPEN431.A4.Utility.MemoryUsage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.zip.CRC32;

import static com.s42442146.CPEN431.A4.model.Command.findCommand;
import static com.s42442146.CPEN431.A4.model.ErrorCode.*;

public class Handler {
    DatagramSocket socket;
    DatagramPacket pkt;
    Cache<ByteBuffer, Message.Msg> cache;
    HashMap<ByteBuffer, Value.Val> KVStore;
    Handler (DatagramSocket socket,
                     DatagramPacket pkt,
                     Cache<ByteBuffer, Message.Msg> cache,
                     HashMap<ByteBuffer, Value.Val> KVStore
                     ) {
        this.socket = socket;
        this.pkt = pkt;
        this.cache = cache;
        this.KVStore = KVStore;
    }

    public void run() {
        try {
            // Request message from client
            Message.Msg requestMessage = Message.Msg.parseFrom
                    (Arrays.copyOfRange(pkt.getData(), 0, pkt.getLength()));
            byte[] id = requestMessage.getMessageID().toByteArray();
            // Request payload from client
            KeyValueRequest.KVRequest reqPayload = KeyValueRequest.KVRequest
                    .parseFrom(requestMessage.getPayload().toByteArray());

            // If cached request, get response msg from cache and send it
            Message.Msg cachedResponse = cache.getIfPresent(ByteBuffer.wrap(id));
            if (cachedResponse != null) {
                sendResponse(cachedResponse);
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

            // Cache request id and response msg
            cache.put(ByteBuffer.wrap(id), responseMsg);

            // Send response to client
            sendResponse(responseMsg);

        } catch (OutOfMemoryError error) {
            System.out.println("Used " + MemoryUsage.getUsedMemory());
            System.out.println("fREE " + MemoryUsage.getFreeMemory());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendResponse (Message.Msg msg) throws IOException {
        byte[] responseAsByteArray = msg.toByteArray();
        DatagramPacket responsePkt = new DatagramPacket(
                responseAsByteArray,
                responseAsByteArray.length,
                pkt.getAddress(),
                pkt.getPort());
        socket.send(responsePkt);
    }

    /*
     *  Generate a response payload
     */
    public KeyValueResponse.KVResponse processRequest(KeyValueRequest.KVRequest requestPayload) {
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
                Value.Val value1 = Value.Val.newBuilder()
                        .setValue(requestPayload.getValue())
                        .setVersion(requestPayload.getVersion())
                        .build();


                    KVStore.put(ByteBuffer.wrap(key),
                            Value.Val.newBuilder()
                                    .setValue(requestPayload.getValue())
                                    .setVersion(requestPayload.getVersion())
                                    .build());
                    return builder
                            .setErrCode(SUCCESSFUL.getCode())
                            .build();

            case GET:

                    Value.Val valueInStore = KVStore.get(ByteBuffer.wrap(key));

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


                    if (!KVStore.containsKey(ByteBuffer.wrap(key))) {
                        return builder
                                .setErrCode(NONEXISTENT_KEY.getCode())
                                .build();
                    }

                    KVStore.remove(ByteBuffer.wrap(key));
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
                        .setPid((int) ProcessHandle.current().pid())   // TODO: this gives a long
                        .build();
            case GET_MEMBERSHIP_COUNT:
                return builder
                        .setErrCode(SUCCESSFUL.getCode())
                        .setMembershipCount(1)
                        .build();
        }
        return builder
                .setErrCode(INTERNAL_FAILURE.getCode())
                .setMembershipCount(1)
                .build();
    }

    private void wipeOut () {
        this.KVStore.clear();
    }

    private boolean isMemoryOverload () {
        return MemoryUsage.getFreeMemory()< 10;
    }
}
