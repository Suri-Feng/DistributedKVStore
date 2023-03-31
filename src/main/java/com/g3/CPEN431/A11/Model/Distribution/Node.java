package com.g3.CPEN431.A11.Model.Distribution;

import com.google.common.hash.Hashing;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class Node {
    private final InetAddress address;
    private final int port;
    private final int id;
    private final int sha256Hash;
    private final int sha512Hash;
    private final int sha384hex;

    public Node(String host, int port, int id) throws UnknownHostException {
        this.address = InetAddress.getByName(host);
        this.port = port;
        String sha256hex = Hashing.sha256()
                .hashString(host+port, StandardCharsets.UTF_8)
                .toString();
        String sha512hex = Hashing.sha512()
                .hashString(host+port, StandardCharsets.UTF_8)
                .toString();
        String sha384hex = Hashing.sha384()
                .hashString(host+port, StandardCharsets.UTF_8)
                .toString();
        this.sha256Hash = sha256hex.hashCode();
        this.sha512Hash = sha512hex.hashCode();
        this.sha384hex = sha384hex.hashCode();
        this.id = id;
    }

    public InetAddress getAddress() {
        return address;
    }
    public int getPort() {
        return port;
    }

    public int getId() {return id;}

    public int getSha256Hash() {
        return sha256Hash;
    }

    public int getSha512Hash() {
        return sha512Hash;
    }

    public int getSha384Hash() {
        return sha384hex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return id == node.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
