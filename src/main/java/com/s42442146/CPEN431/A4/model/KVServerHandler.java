package com.s42442146.CPEN431.A4.model;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;

import ca.NetSysLab.ProtocolBuffers.Value;
import ca.NetSysLab.ProtocolBuffers.Message;

import com.github.benmanes.caffeine.cache.Cache;
import com.s42442146.CPEN431.A4.Utility.MemoryUsage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.zip.CRC32;

import static com.s42442146.CPEN431.A4.model.Command.*;
import static com.s42442146.CPEN431.A4.model.ErrorCode.*;


public class KVServerHandler implements Runnable {
    DatagramSocket socket;
    Message.Msg requestMessage;
    Cache<ByteBuffer, Message.Msg> cache;
    InetAddress address;
    int port;
    KVStore store = KVStore.getInstance();
    KVServerHandler(Message.Msg requestMessage,
                    DatagramSocket socket,
                    Cache<ByteBuffer, Message.Msg> cache,
                    InetAddress address,
                    int port) {
        this.socket = socket;
        this.cache = cache;
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


            sendResponse(responseMsg);
            resizeCache();
            cache.put(ByteBuffer.wrap(id), responseMsg);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendResponse(Message.Msg msg) throws IOException {
        byte[] responseAsByteArray = msg.toByteArray();
        DatagramPacket responsePkt = new DatagramPacket(
                responseAsByteArray,
                responseAsByteArray.length,
                address,
                port);
        socket.send(responsePkt);
    }

    private synchronized void resizeCache () {
        cache.cleanUp();
        cache.policy().eviction().ifPresent(eviction -> {
            if (eviction.getMaximum() != KVServer.DEFAULT_CACHE_SIZE &&
                    cache.estimatedSize() < KVServer.DEFAULT_CACHE_SIZE * 2/ 3) {
                eviction.setMaximum(KVServer.DEFAULT_CACHE_SIZE);
                System.out.println("Resizing cache size to 500.");
                System.out.println("===================================");
            } else if (cache.estimatedSize() >= (eviction.getMaximum() * 2 / 3)) {
                System.out.println("Current size: " + eviction.getMaximum());
                eviction.setMaximum(2 * eviction.getMaximum());
                System.out.println("Resizing cache size to: " + eviction.getMaximum());
                System.out.println("===================================");
            }
        });
    }

    /*
     *  Generate a response payload
     */
    public  KeyValueResponse.KVResponse processRequest(KeyValueRequest.KVRequest requestPayload) {
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

                store.getStore().put(ByteBuffer.wrap(key),
                        Value.Val.newBuilder()
                                .setValue(requestPayload.getValue())
                                .setVersion(requestPayload.getVersion())
                                .build());

                return builder
                        .setErrCode(SUCCESSFUL.getCode())
                        .build();
            case GET:
                Value.Val valueInStore = store.getStore().get(ByteBuffer.wrap(key));
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
                .build();
    }

    private  void wipeOut() {
        store.clearStore();
        cache.invalidateAll();
        cache.policy().eviction().ifPresent(eviction -> eviction.setMaximum(KVServer.DEFAULT_CACHE_SIZE));
    }

    private boolean isMemoryOverload() {
        return MemoryUsage.getFreeMemory() < 0.05 * MemoryUsage.getMaxMemory();
    }
}


