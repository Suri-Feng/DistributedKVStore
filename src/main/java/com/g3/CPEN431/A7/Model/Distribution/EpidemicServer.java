package com.g3.CPEN431.A7.Model.Distribution;


import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.g3.CPEN431.A7.Model.Command;

import java.io.IOException;
import java.net.*;
import java.util.Collection;
import java.util.Random;
import java.util.zip.CRC32;


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
        if (nodesCircle.getAliveNodesCount() == 1) {
            return;
        }
        heartbeatsManager.getHeartBeats().put(myNodeId, System.currentTimeMillis());

        byte[] requestBytes = packMessage();

        for (int i = 0; i < NUM_NEIGHBOURS; i++) {
            int randomInt1;
            Node randomNode1;

            do {
                randomInt1 = r.nextInt(nodesCircle.getStartupNodesSize());
                randomNode1 = nodesCircle.getNodeById(randomInt1);
            } while (randomNode1.getId() == myNodeId);

//            System.out.println("Sending gossip to: " + randomNode1.getPort());
            DatagramPacket packet = new DatagramPacket(
                    requestBytes,
                    requestBytes.length,
                    randomNode1.getAddress(),
                    randomNode1.getPort());

            try {
                socket.send(packet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private byte[] packMessage() {
        // messageID
        byte[] msg_id = new byte[16];
        Collection<Long> heartbeats = heartbeatsManager.getHeartBeats().values();
        KeyValueRequest.KVRequest gossip = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Command.HEARTBEAT.getCode())
                .addAllHeartbeat(heartbeats)
                .build();

        // Checksum
        CRC32 checksum = new CRC32();
        checksum.update(msg_id);
        checksum.update(gossip.toByteArray());

        // Create the message
        Message.Msg requestMessage = Message.Msg.newBuilder()
                .setMessageID(ByteString.copyFrom(msg_id))
                .setPayload(gossip.toByteString())
                .setCheckSum(checksum.getValue())
                .build();

        return requestMessage.toByteArray();
    }
}
