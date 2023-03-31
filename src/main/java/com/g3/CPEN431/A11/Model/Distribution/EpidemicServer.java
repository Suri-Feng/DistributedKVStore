package com.g3.CPEN431.A11.Model.Distribution;


import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.g3.CPEN431.A11.Model.Command;

import java.io.IOException;
import java.net.*;
import java.util.Collection;
import java.util.Random;


public class EpidemicServer implements Runnable {
    public static final int NUM_NEIGHBOURS = 2;
    private final NodesCircle nodesCircle = NodesCircle.getInstance();
    private final HeartbeatsManager heartbeatsManager = HeartbeatsManager.getInstance();
    private final DatagramSocket socket;
    private final int myNodeId;
    private final Random r;

    public EpidemicServer(DatagramSocket socket, int myNodeId) {
        this.socket = socket;
        this.myNodeId = myNodeId;
        r = new Random();
    }

    @Override
    public void run() {
        // if only the current node is alive, no need to gossip
        // I want to process information from other node faster, instead of sending info to other nodes (should already sned)
//        if (nodesCircle.getAliveNodesCount() == 1) {
//           System.out.println(KVServer.port + " suspended node should not have alive list = 1");
//            heartbeatsManager.getHeartBeats().put(myNodeId, System.currentTimeMillis());
//            return;
//        }

        for (int i = 0; i < NUM_NEIGHBOURS; i++) {
            int randomInt;
            Node randomNode;


            // Alive nodes are my priority, since recovered nodes will contact me first
            // If the node chosen is not on the ring, I would prefer not sending to it
            // No dead node socket congestion & faster communication with alive nodes
            do {
                randomInt = r.nextInt(nodesCircle.getStartupNodesSize());
                randomNode = nodesCircle.getNodeById(randomInt);
            } while (randomNode.getId() == myNodeId && !heartbeatsManager.isNodeAlive(randomNode));

            heartbeatsManager.getHeartBeats().put(myNodeId, System.currentTimeMillis());
            byte[] requestBytes = packMessage();
            DatagramPacket packet = new DatagramPacket(
                    requestBytes,
                    requestBytes.length,
                    randomNode.getAddress(),
                    randomNode.getPort());

            try {
                if (socket.getLocalPort() > 30000 || socket.getPort() > 30000)
                    System.out.println("Get local port " + socket.getLocalPort() + ", get port " + socket.getPort());
                socket.send(packet);
            } catch (IOException e) {
                System.out.println("====================");
                System.out.println("[ Epidemic, " + socket.getLocalPort() + ", " + Thread.currentThread().getName() + "]: "
                        + e.getMessage() + randomNode.getPort());
                System.out.println("====================");
                throw new RuntimeException(e);
            }
        }
    }

    private byte[] packMessage() {
        //ByteString messageID = generateMessageID(socket);

        byte[] messageID = new byte[1];
        Collection<Long> heartbeats = heartbeatsManager.getHeartBeats().values();
        KeyValueRequest.KVRequest gossip = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Command.HEARTBEAT.getCode())
                .addAllHeartbeat(heartbeats)
                .build();

        // Create the message
        Message.Msg requestMessage = Message.Msg.newBuilder()
                .setMessageID(ByteString.copyFrom(messageID))
                .setPayload(gossip.toByteString())
                .setCheckSum(0)
                .build();

        return requestMessage.toByteArray();
    }
}
