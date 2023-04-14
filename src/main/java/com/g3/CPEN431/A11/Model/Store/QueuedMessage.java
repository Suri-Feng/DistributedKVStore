package com.g3.CPEN431.A11.Model.Store;

import com.google.protobuf.ByteString;

import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class QueuedMessage {
    InetAddress address;
    int port;
    Set<Integer> ackPorts;
    ByteString id;
    ByteString key;
    int version;
    ByteString value;

    public QueuedMessage(InetAddress address, int port, ByteString id, ByteString key, ByteString value, int version) {
        this.address = address;
        this.port = port;
        this.ackPorts = ConcurrentHashMap.newKeySet();
        this.id = id;
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public void addAckPort(int port) {
        this.ackPorts.add(port);
    }

    public int getAckPortsCount() {
        return this.ackPorts.size();
    }

    public int getPort() {
        return port;
    }

    public InetAddress getAddress() {
        return address;
    }

    public ByteString getId() {
        return id;
    }

    public int getVersion() {
        return version;
    }

    public ByteString getKey() {
        return key;
    }

    public ByteString getValue() {
        return value;
    }

//    @Override
//    public int compareTo(QueuedMessage o) {
//        if (this.timestamp < o.timestamp) {
//            return -1;
//        } else if (this.timestamp > o.timestamp) {
//            return 1;
//        } else {
//            return 0;
//        }
//    }
}
