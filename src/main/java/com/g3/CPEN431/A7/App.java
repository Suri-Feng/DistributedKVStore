package com.g3.CPEN431.A7;

import com.g3.CPEN431.A7.Model.Distribution.Node;
import com.g3.CPEN431.A7.Model.Distribution.NodesCircle;
import com.g3.CPEN431.A7.Model.KVServer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class App {
    public static void main( String[] args ) {
        try {
            ArrayList<Node> nodes = parseNodes();
            NodesCircle.getInstance().setNodeList(nodes);
            NodesCircle.getInstance().buildHashCircle();
            new KVServer(Integer.parseInt(args[0])).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ArrayList<Node> parseNodes() throws IOException {
        ArrayList<Node> nodes = new ArrayList<>();
        FileReader fileReader = new FileReader("nodes-list.txt");
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line = bufferedReader.readLine();
        int id = 0;
        while (line != null) {
            String[] args = line.split(":");
            Node node = new Node(args[0], Integer.parseInt(args[1]), id++);
            nodes.add(node);
            line = bufferedReader.readLine();
        }

        bufferedReader.close();
        return nodes;
    }
}
