package com.g3.CPEN431.project.Client;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g3.CPEN431.project.ServerInfo.Server;
import com.g3.CPEN431.project.Test.OutcomePair;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.net.*;

import static com.g3.CPEN431.project.Client.MessageBuilder.buildKVRequest;
import static com.g3.CPEN431.project.Utils.NetUtils.*;
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

    public OutcomePair run(Server server, KeyValueRequest.KVRequest request) throws IOException, InterruptedException {
        OutcomePair outcome = null;
        int retries = 0;
        int delay = 100;
        int errCode = -1; // -1 is invalid
        for (; retries <= 3; retries++) {
            //System.out.println("======Number of retries: " + retries + "=====");

            if (retries == 0) {
                messageID = generateMessageID(socket);
                packet = pack(server, request);
            }
            socket.send(packet);
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
                    } else if (response.hasPid()) {
                        server.setPid(response.getPid());
                        outcome = new OutcomePair(OutcomePair.Status.SUCCESS, "Received pid " + response.getPid() + " , and set to server " + server.getPort());
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
                    packet = pack(server, request);
                    outcome = new OutcomePair(OutcomePair.Status.TIMEOUT, "");
                    break;
                case 3: // overload -> resend after xxx seconds
                    System.out.println("Overload, client xxx sleeps for "+ response.getOverloadWaitTime() +"ms");
                    sleep(response.getOverloadWaitTime());
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


    private DatagramPacket pack(Server server, KeyValueRequest.KVRequest request) {
        Message.Msg msg = Message.Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(request.toByteString())
                .setCheckSum(getChecksum(messageID.toByteArray(), request.toByteArray()))
                .build();

        return new DatagramPacket(msg.toByteArray(), msg.getSerializedSize(), server.getAddress(), server.getPort());
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
