package com.g3.CPEN431.A7.Model.Distribution;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class Node {
    private InetAddress address;
    private int port;
    private long hash;
    private int id;
    public Node(String host, int port, int id) throws UnknownHostException {
        this.address = InetAddress.getByName(host);
        this.port = port;
        this.hash = hashTo64bit(host + port);
        this.id = id;
    }

    public InetAddress getAddress() {
        return address;
    }
    public int getPort() {
        return port;
    }
    public long getHash() {
        return hash;
    }

    public int getId() {return id;}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return port == node.port && address.equals(node.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
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
