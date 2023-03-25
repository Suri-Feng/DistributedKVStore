package com.g3.CPEN431.A9.Utility;
import com.google.protobuf.ByteString;

import java.net.DatagramSocket;
import java.util.Random;
import java.util.zip.CRC32;

public class NetUtils {
    public static long getChecksum(byte[] A, byte[] B) {
        byte[] checksumInput = new byte[A.length + B.length];
        System.arraycopy(A, 0, checksumInput, 0, A.length);
        System.arraycopy(B, 0, checksumInput, A.length, B.length);
        CRC32 crc32;
        crc32 = new CRC32();
        crc32.update(checksumInput);
        return crc32.getValue();
    }

    public static ByteString generateMessageID(DatagramSocket socket) {
        long sendTime = System.nanoTime();
        Random rand = new Random();
        short int_random = (short)rand.nextInt(1 << 15); // generate a non-negative short

        byte[] buf = new byte[16];
        byte[] ip_bytes = socket.getLocalAddress().getAddress();
        System.arraycopy(ip_bytes, 0, buf, 0, 4);
        ByteOrder.short2beb((short)socket.getLocalPort(), buf, 4);
        ByteOrder.short2beb(int_random, buf, 6);
        ByteOrder.long2beb(sendTime, buf, 8);
        return  ByteString.copyFrom(buf);
    }
}