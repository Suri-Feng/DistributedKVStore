package com.g3.CPEN431.A9.Model.Distribution;


import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.g3.CPEN431.A9.Model.Command;

import java.io.IOException;
import java.net.*;
import java.util.Collection;
import java.util.Random;

import static com.g3.CPEN431.A9.Utility.NetUtils.generateMessageID;
import static com.g3.CPEN431.A9.Utility.NetUtils.getChecksum;


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
//        if (nodesCircle.getAliveNodesCount() == 1) {
//            return;
//        }

        for (int i = 0; i < NUM_NEIGHBOURS; i++) {
            int randomInt;
            Node randomNode;

            do {
                randomInt = r.nextInt(nodesCircle.getStartupNodesSize());
                randomNode = nodesCircle.getNodeById(randomInt);
            } while (randomNode.getId() == myNodeId);

            heartbeatsManager.getHeartBeats().put(myNodeId, System.currentTimeMillis());
            byte[] requestBytes = packMessage();
            DatagramPacket packet = new DatagramPacket(
                    requestBytes,
                    requestBytes.length,
                    randomNode.getAddress(),
                    randomNode.getPort());

            try {
                socket.send(packet);
            } catch (IOException e) {
                System.out.println("====================");
                System.out.println("[ Epidemic, "+socket.getLocalPort()+", " + Thread.currentThread().getName() + "]: "
                        + e.getMessage() + randomNode.getPort());
                System.out.println("====================");
                throw new RuntimeException(e);
            }
        }
    }

    private byte[] packMessage() {
        ByteString messageID = generateMessageID(socket);
        Collection<Long> heartbeats = heartbeatsManager.getHeartBeats().values();
        KeyValueRequest.KVRequest gossip = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Command.HEARTBEAT.getCode())
                .addAllHeartbeat(heartbeats)
                .build();

        // Create the message
        Message.Msg requestMessage = Message.Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(gossip.toByteString())
                .setCheckSum(getChecksum(messageID.toByteArray(),gossip.toByteArray()))
                .build();

        return requestMessage.toByteArray();
    }
}
