package com.g3.CPEN431.project.Utils;

import com.google.protobuf.ByteString;

import java.net.DatagramSocket;
import java.util.zip.CRC32;

public class General {
    public static long getChecksum(byte[] A, byte[] B) {
        byte[] checksumInput = new byte[A.length + B.length];
        System.arraycopy(A, 0, checksumInput, 0, A.length);
        System.arraycopy(B, 0, checksumInput, A.length, B.length);
        CRC32 crc32;
        crc32 = new CRC32();
        crc32.update(checksumInput);
        return crc32.getValue();
    }


}
