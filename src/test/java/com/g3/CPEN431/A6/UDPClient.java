package com.g3.CPEN431.A6;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.net.*;
import java.util.zip.CRC32;

public class UDPClient {
    DatagramSocket socket;
    DatagramPacket packet;
    ByteString messageID;

    public void createSocket() {
        try {
            socket = new DatagramSocket();
        } catch (SocketException e) {
            System.out.println("Error creating socket: " + e);
        }
    }

    public void changeSocketTimeout(int time) {
        try {
            socket.setSoTimeout(time);
        } catch (SocketException e) {
            System.out.println("Error setting socket timeout: " + e);
        }
    }

    public void sendMsg(InetAddress host, int port, KeyValueRequest.KVRequest request, boolean retry) {
        try {
            if (!retry) {
                packet = pack(host, port, request);
            }

            socket.send(packet);
            System.out.println("Sent message to " + host + ":" + port);
        } catch (IOException e) {
            System.out.println("Error sending packet: " + e);
        }
    }

    private DatagramPacket pack(InetAddress ip, int port, KeyValueRequest.KVRequest request) {
        byte[] buf = new byte[16];
        byte[] ip_bytes = socket.getLocalAddress().getAddress();
        System.arraycopy(ip_bytes, 0, buf, 0, 4);
        short2beb((short)socket.getLocalPort(), buf, 4);
        messageID = ByteString.copyFrom(buf);

        // Change into new method
        KeyValueRequest.KVRequest payload = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(0x00)
                .build();

        Message.Msg msg = Message.Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(request.toByteString())
                .setCheckSum(getChecksum(buf, request.toByteArray()))
                .build();

        return new DatagramPacket(msg.toByteArray(), msg.getSerializedSize(), ip, port);
    }

    private static long getChecksum(byte[] A, byte[] B) {
        byte[] checksumInput = new byte[A.length + B.length];
        System.arraycopy(A, 0, checksumInput, 0, A.length);
        System.arraycopy(B, 0, checksumInput, A.length, B.length);
        CRC32 crc32;
        crc32 = new CRC32();
        crc32.update(checksumInput);
        return crc32.getValue();
    }

    private static void short2beb(short x, byte[] buf, int offset) {
        buf[offset+1]=(byte)(x & 0x000000FF);
        buf[offset]=(byte)((x>>8) & 0x000000FF);
    }
    public static void long2beb(long x, byte[] buf, int offset) {
        for (int i = 0; i < 8; i += 1) {
            buf[offset + 7 - i] = (byte)((x>>(8*i)) & 0xFF);
        }
    }

    public KeyValueResponse.KVResponse receiveMsg() throws SocketTimeoutException {
        DatagramPacket recvPacket =
                new DatagramPacket(new byte[1024],  1024);
        KeyValueResponse.KVResponse reply = null;

        try {
            socket.receive(recvPacket);

            System.out.println("Received message from " +
                    recvPacket.getAddress() +
                    ":" + recvPacket.getPort());

            byte[] bytes = new byte[recvPacket.getLength()];
            System.arraycopy(recvPacket.getData(), 0, bytes, 0, recvPacket.getLength());
            reply = unpack(bytes);
        } catch (SocketTimeoutException e) {
            System.out.println("Exception: Socket time out");
        } catch (IOException e) {
            System.out.println("Error reading from socket: " + e);
        }
        return reply;
    }

    private KeyValueResponse.KVResponse unpack(byte[] bytes) throws InvalidProtocolBufferException {

        Message.Msg msg = Message.Msg.parseFrom(bytes);
        ByteString recvMessageID = msg.getMessageID();
        KeyValueResponse.KVResponse resPayload = KeyValueResponse.KVResponse.parseFrom(msg.getPayload());
        long recvChecksum = msg.getCheckSum();

        // Check header
        if (!recvMessageID.equals(messageID)) {
            System.out.println("Exception: Received message ID is not the same");
            return null;
        }

        // Check checksum
        if (recvChecksum != getChecksum(recvMessageID.toByteArray(), resPayload.toByteArray())) {
            System.out.println("Exception: Received checksum is not the same.");
            return null;
        }

        //System.out.println("Secret Code Length: " + resPayload.getSecretKey().size());
        //System.out.println("Secret Code: " + StringUtils.byteArrayToHexString(resPayload.getSecretKey().toByteArray()));

        return resPayload;
    }
}
