package com.g3.CPEN431.project.ServerInfo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class Server {
    private final InetAddress address;
    private final int port;
    private int membershipId;
    private int pid;
    public Server(String host, int port) throws UnknownHostException {
        this.address = InetAddress.getByName(host);
        this.port = port;
    }

    public InetAddress getAddress() {
        return address;
    }
    public int getPort() {
        return port;
    }

    public void setMembershipId (int membershipId) {
        this.membershipId = membershipId;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getMembershipId() {
        return this.membershipId;
    }

    public int getPid() {
        return this.pid;
    }

}
