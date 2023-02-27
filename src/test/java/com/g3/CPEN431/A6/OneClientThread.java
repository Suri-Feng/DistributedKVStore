package com.g3.CPEN431.A6;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Queue;

public class OneClientThread {
    public static void run(String remote_host, int remote_port, Queue<KeyValueRequest.KVRequest> requests) throws SocketTimeoutException, UnknownHostException, InterruptedException {
        System.out.println("Contacting host " + InetAddress.getByName(remote_host) + " at port " + remote_port);

        UDPClient Client = new UDPClient();
        Client.createSocket();

        for (KeyValueRequest.KVRequest request: requests) {
            int retries = 0;
            int delay = 100;
            for (; retries <= 3; retries++) {
                System.out.println("======Number of retries: " + retries + "=====");
                Client.sendMsg(InetAddress.getByName(remote_host), remote_port, request, retries > 0);
                Client.changeSocketTimeout(delay);
                if (Client.receiveMsg() == null) {
                    delay *= 2;
                } else {
                    break;
                }
            }

            if (retries == 4) {
                System.out.println("1 attempt and 3 retries failed");
                System.exit(-1);
            }
            System.exit(0);
        }
    }
}
