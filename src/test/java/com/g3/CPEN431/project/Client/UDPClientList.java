package com.g3.CPEN431.project.Client;

import com.g3.CPEN431.project.ServerInfo.Server;

import java.io.IOException;
import java.util.ArrayList;

public class UDPClientList {
    public ArrayList<UDPClient> clients;
    public int num;


    public UDPClientList(int num) throws IOException {
        this.clients = new ArrayList<>();
        this.num = num;

        for(int i = 0; i < num; i ++) {
            UDPClient client = new UDPClient();
            client.createSocket();
            this.clients.add(client);
        }
    }

    public void addClient(int n) {
        this.num += n;
        for(int i = 0; i < n; i ++) {
            UDPClient client = new UDPClient();
            client.createSocket();
            this.clients.add(client);
        }
    }

    public void removeClient(int n) {
        this.num -= n;
        if (this.num <= 0) {
            System.out.println("Can not remove " + n + " clients");
            this.num += n;
            return;
        }
        for(int i = 0; i < n; i ++) {
            UDPClient client = new UDPClient();
            client.createSocket();
            this.clients.remove(0);
        }
    }

    public UDPClient getFirstClient() {
        return this.clients.get(0);
    }

    public int size() {
        return this.num;
    }


}
