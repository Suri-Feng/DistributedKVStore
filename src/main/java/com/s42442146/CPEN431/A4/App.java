package com.s42442146.CPEN431.A4;

import com.s42442146.CPEN431.A4.model.Distribution.NodesCircle;
import com.s42442146.CPEN431.A4.model.KVServer;
import com.s42442146.CPEN431.A4.model.Distribution.Node;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class App {
    public static void main( String[] args ) {
        try {
            ArrayList<Node> nodes = parseNodes();

            int numNodes = nodes.size();
            int n = 1 << numNodes;
            for (Node node: nodes) {
                int hash = (int) (node.getId() % n < 0 ? node.getId() % n + n : node.getId() % n);
                NodesCircle.getInstance().getNodesTable().put(hash, node);
            }

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
        while (line != null) {
            String[] args = line.split(":");
            Node node = new Node(args[0], Integer.parseInt(args[1]));
            nodes.add(node);
            line = bufferedReader.readLine();
        }

        bufferedReader.close();
        return nodes;
    }
}
