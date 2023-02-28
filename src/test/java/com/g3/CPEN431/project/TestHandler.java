package com.g3.CPEN431.project;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import static com.g3.CPEN431.project.MessageBuilder.Commands;
import static com.g3.CPEN431.project.MessageBuilder.buildKVRequest;


public class TestHandler {

    public static final String SERVER_ADDR = "128.189.136.206";

//    static {
//        try {
//            SERVER_ADDR = InetAddress.getLocalHost().getHostAddress();
//        } catch (UnknownHostException e) {
//            throw new RuntimeException(e);
//        }
//    }

    private static final int SERVER_PORT = 12345;

    public static void printOutcome(OutcomePair outcomePair) {
        System.out.println("Outcome: [ " + outcomePair.getStatus() +", "+ outcomePair.getValue() +" ]");
    }
    public static OutcomePair put(String key, String val, int version, UDPClient client) throws SocketTimeoutException, UnknownHostException, InterruptedException {
        KeyValueRequest.KVRequest request = buildKVRequest(Commands.PUT, key, val, version);
        return client.run(SERVER_ADDR, SERVER_PORT, request);
    }

    public static OutcomePair get(String key, UDPClient client) throws SocketTimeoutException, UnknownHostException, InterruptedException {
        KeyValueRequest.KVRequest request = buildKVRequest(Commands.GET, key);
        return client.run(SERVER_ADDR, SERVER_PORT, request);
    }

    public static OutcomePair remove(String key, UDPClient client) throws SocketTimeoutException, UnknownHostException, InterruptedException {
        KeyValueRequest.KVRequest request = buildKVRequest(Commands.REMOVE, key);
        return client.run(SERVER_ADDR, SERVER_PORT, request);
    }

    public static OutcomePair shutdown(UDPClient client) throws SocketTimeoutException, UnknownHostException, InterruptedException {
        KeyValueRequest.KVRequest request = buildKVRequest(Commands.SHUTDOWN);
        return client.run(SERVER_ADDR, SERVER_PORT, request); // retry 3 times, should receive timeout
    }

    public static OutcomePair isAlive(UDPClient client) throws SocketTimeoutException, UnknownHostException, InterruptedException {
        KeyValueRequest.KVRequest request = buildKVRequest(Commands.IS_ALIVE);
        return client.run(SERVER_ADDR, SERVER_PORT, request); // should receive success
    }

    public static OutcomePair wipeOut(UDPClient client) throws SocketTimeoutException, UnknownHostException, InterruptedException {
        KeyValueRequest.KVRequest request = buildKVRequest(Commands.WIPE_OUT);
        return client.run(SERVER_ADDR, SERVER_PORT, request); // should receive success
    }
}
