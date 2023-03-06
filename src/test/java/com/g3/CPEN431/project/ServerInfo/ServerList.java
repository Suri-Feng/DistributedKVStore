package com.g3.CPEN431.project.ServerInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import static java.lang.System.exit;

public class ServerList {
    public ArrayList<Server> servers;
    public int num;



    public ServerList(String filename) throws IOException {
       this.servers = new ArrayList<>();
        FileReader fileReader = new FileReader(filename);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line = bufferedReader.readLine();

        this.num = 0;
        while (line != null)
        {
            num ++;
            String[] args = line.split(":");
            Server node = new Server(args[0], Integer.parseInt(args[1]));
            this.servers.add(node);
            line = bufferedReader.readLine();
        }

        if (this.num == 0) {
            System.out.println("No node is listed in the servers list file. Abort.");
            exit(0);
        }

        bufferedReader.close();
    }

    public Server getFirstServer() {
        return this.servers.get(0);
    }

    public int size() {
        return this.num;
    }
}
