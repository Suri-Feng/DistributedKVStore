package com.g3.CPEN431.A7.Utility;

/**
 * Various static routines to help with strings
 */
public class StringUtils {

    public static String byteArrayToHexString(byte[] bytes) {
        StringBuffer buf=new StringBuffer();
        String       str;
        int val;

        for (int i=0; i<bytes.length; i++) {
            val = ByteOrder.ubyte2int(bytes[i]);
            str = Integer.toHexString(val);
            while ( str.length() < 2 )
                str = "0" + str;
            buf.append( str );
        }
        return buf.toString().toUpperCase();
    }

    public static byte[] hexStringToByteArray(String hexString) {
        int len = hexString.length();
        byte[] byteArray = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            byteArray[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i+1), 16));
        }
        return byteArray;
    }
}