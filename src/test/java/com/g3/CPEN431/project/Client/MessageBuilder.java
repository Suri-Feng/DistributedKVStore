package com.g3.CPEN431.project.Client;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import com.google.protobuf.ByteString;

public class MessageBuilder {
    public enum Commands {
        INVALID,
        PUT,
        GET,
        REMOVE,
        SHUTDOWN,
        WIPE_OUT,
        IS_ALIVE,
        GET_PID,
        GET_MEMBERSHIP_COUNT
    }
    public static KeyValueRequest.KVRequest buildKVRequest(Commands command, String key, String val, int version) {
        assert (command == Commands.PUT);
        System.out.println("[ Sending request: " + command + ", key " + key + ", val " + val + ", version " + version +" ]");
        return KeyValueRequest.KVRequest.newBuilder()
                    .setCommand(Commands.PUT.ordinal())
                    .setKey(ByteString.copyFrom(key.getBytes()))
                    .setValue(ByteString.copyFrom(val.getBytes()))
                    .setVersion(version)
                    .build();
    }

    public static KeyValueRequest.KVRequest buildKVRequest(Commands command, String key) {
        assert (command == Commands.GET || command == Commands.REMOVE);
        System.out.println("[ Sending request: " + command + ", " + key + " ]");
        return KeyValueRequest.KVRequest.newBuilder()
                .setCommand(command.ordinal())
                .setKey(ByteString.copyFrom(key.getBytes()))
                .build();
    }

    public static KeyValueRequest.KVRequest buildKVRequest(Commands command) {
        assert (command == Commands.SHUTDOWN || command == Commands.WIPE_OUT ||
                command == Commands.IS_ALIVE || command == Commands.GET_PID || command == Commands.GET_MEMBERSHIP_COUNT);
        System.out.println("[ Sending request: " + command + " ]");
        return KeyValueRequest.KVRequest.newBuilder()
                .setCommand(command.ordinal())
                .build();
    }
}
