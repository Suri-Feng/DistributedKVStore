package com.s42442146.CPEN431.A4.model.Distribution;


import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Random;
import java.util.zip.CRC32;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class EpidemicServer implements Runnable {
    private NodesCircle nodesCircle = NodesCircle.getInstance();
    private HeartbeatsManager heartbeatsManager = HeartbeatsManager.getInstance();
    private DatagramSocket socket;
    private int myNodeId;
    private Random r;

    public EpidemicServer(DatagramSocket socket, int myNodeId) {
        this.socket = socket;
        this.myNodeId = myNodeId;
        r = new Random();
    }

    @Override
    public void run() {
        System.out.println("Epidemic Server running");
        heartbeatsManager.getHeartBeats().put(myNodeId, System.currentTimeMillis());

        int randomInt1;
        Node randomNode1;

        do {
            randomInt1 = r.nextInt(nodesCircle.getStartupNodesSize());
            System.out.println(randomInt1);
            randomNode1 = nodesCircle.getNodeById(randomInt1);
        } while (randomNode1.getId() == myNodeId);

        System.out.println("Sending to node port: " + randomInt1);

        byte[] requestBytes = packMessage();

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

    private byte[] packMessage() {
        // messageID
        byte[] msg_id = new byte[16];
        try {
            getUniqueID(msg_id);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        // TODO: heartbeatsManager.getHeartBeats().values();
        Collection<Long> heartbeats = heartbeatsManager.getHeartBeats().values();
        KeyValueRequest.KVRequest gossip = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(10)
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

    private void getUniqueID (byte[] msg_id) throws UnknownHostException {
        ByteBuffer msg_id_buf = ByteBuffer.wrap(msg_id);
        msg_id_buf.order(LITTLE_ENDIAN);

        msg_id_buf.put(InetAddress.getLocalHost().getAddress());
        msg_id_buf.putShort((short)socket.getLocalPort());
        msg_id_buf.putShort((short) new Random().nextInt());
        msg_id_buf.putLong(System.nanoTime());
    }
}
