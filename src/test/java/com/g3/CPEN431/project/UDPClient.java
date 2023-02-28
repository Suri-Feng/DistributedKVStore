package com.g3.CPEN431.project;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.project.Utils.ByteOrder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.net.*;
import java.util.LinkedList;
import java.util.Random;

import static com.g3.CPEN431.project.MessageBuilder.buildKVRequest;
import static com.g3.CPEN431.project.Utils.General.*;
import static java.lang.Thread.sleep;

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

    public OutcomePair run(String remote_ho, int remote_port, KeyValueRequest.KVRequest request) throws SocketTimeoutException, UnknownHostException, InterruptedException {
        InetAddress remote_host = InetAddress.getByName(remote_ho);
        OutcomePair outcome = null;
        int retries = 0;
        int delay = 100;
        int errCode = -1; // -1 is invalid
        for (; retries <= 3; retries++) {
            //System.out.println("======Number of retries: " + retries + "=====");
            sendMsg(remote_host, remote_port, request, retries > 0);
            changeSocketTimeout(delay);
            delay *= 2;
            KeyValueResponse.KVResponse response = receiveMsg();
            if (response == null) {
                //System.out.println("Packet lost or corrupted");
                continue;
            }
            errCode = response.getErrCode();
            switch (errCode) {
                case 0:
                    if (response.hasValue()) {
                        outcome = new OutcomePair(OutcomePair.Status.SUCCESS, response.getValue().toString());
                    } else {
                        outcome =  new OutcomePair(OutcomePair.Status.SUCCESS, "");
                    }
                    break;
                case 1:
                    outcome = new OutcomePair(OutcomePair.Status.KEYNOTFOUND, "");
                    break;
                case 2: // out-of-space -> send wipe-out, resend after xxx seconds, outcome would be TIMEOUT
                    System.out.println("Out-of-space, sending shut down, sleep for 1s");
                    request = buildKVRequest(MessageBuilder.Commands.SHUTDOWN);
                    packet = pack(remote_host, remote_port, request);
                    outcome = new OutcomePair(OutcomePair.Status.TIMEOUT, "");
                    break;
                case 3: // overload -> resend after xxx seconds
                    System.out.println("Overload, client xxx sleeps for "+ response.getOverloadWaitTime() +"ms");
                    Thread.sleep(response.getOverloadWaitTime());
                    continue;
                default:
                    System.out.println("Invalid errCode: " + errCode);
                    System.out.println(response.hasValue());
                    break;
            }
            break;
        }

        // All retries faield (1 attempt and 3 retries) -> timeout
        if (retries == 4) {
            System.out.println("Timeout, current error code: " + errCode);
            outcome = new OutcomePair(OutcomePair.Status.TIMEOUT, "");
        }
        return outcome;
    }

    public void sendMsg(InetAddress host, int port, KeyValueRequest.KVRequest request, boolean retry) {
        try {
            if (!retry) {
                packet = pack(host, port, request);
            }

            socket.send(packet);
            //System.out.println("Sent message to " + host + ":" + port);
        } catch (IOException e) {
            System.out.println("Error sending packet: " + e);
        }
    }

    private DatagramPacket pack(InetAddress ip, int port, KeyValueRequest.KVRequest request) {
        messageID = generateMessageID(socket);

        Message.Msg msg = Message.Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(request.toByteString())
                .setCheckSum(getChecksum(messageID.toByteArray(), request.toByteArray()))
                .build();

        return new DatagramPacket(msg.toByteArray(), msg.getSerializedSize(), ip, port);
    }



    public KeyValueResponse.KVResponse receiveMsg()  {
        DatagramPacket recvPacket =
                new DatagramPacket(new byte[13000],  13000);

        KeyValueResponse.KVResponse reply = null;

        try {
            socket.receive(recvPacket);

//            System.out.println("Received message from " +
//                    recvPacket.getAddress() +
//                    ":" + recvPacket.getPort());

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

//         // TODO Didnt get response
//        if (resPayload.getSerializedSize() == 0) {
//            System.out.println("Exception: Nothing in KVResponse");
//            return null;
//        }

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

        return resPayload;
    }
}
