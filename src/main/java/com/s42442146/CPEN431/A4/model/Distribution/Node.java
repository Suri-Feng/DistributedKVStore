package com.s42442146.CPEN431.A4.model.Distribution;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Node {
    private InetAddress address;
    private int port;
    private long id;
    public Node(String host, int port) throws UnknownHostException {
        this.address = InetAddress.getByName(host);
        this.port = port;
        this.id = hashTo64bit(host + port);
    }

    public InetAddress getAddress() {
        return address;
    }
    public int getPort() {
        return port;
    }
    public long getId() {
        return id;
    }

    /**
     * Converts Strings to 64-bit longs
     * http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
     * @param string String to hash to 64-bit
     * @return
     */
    public static long hashTo64bit(String string) {
        // Take a large prime
        long h = 1125899906842597L;
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = 31*h + string.charAt(i);
        }
        return h;
    }
}
