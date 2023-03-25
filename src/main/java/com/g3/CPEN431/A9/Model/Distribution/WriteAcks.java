package com.g3.CPEN431.A9.Model.Distribution;

import com.google.protobuf.ByteString;

import java.net.InetAddress;

public class WriteAcks {
    InetAddress clientAddress;
    Integer clientPort;
    Integer Acks;

    public WriteAcks(InetAddress clientAddress, Integer clientPort) {
        this.clientAddress = clientAddress;
        this.clientPort = clientPort;
        Acks = 0;
    }

    public void updateAck() {
        Acks += 1;
        if(Acks > 3) {
            System.out.println("Error: Received more than 3 acks from 3 backups");
        }
    }

    public boolean allBackupsAcked() {
        return Acks >= 3;
    }

    public InetAddress getClientAddress() {
        return clientAddress;
    }

    public Integer getClientPort() {
        return clientPort;
    }
}
