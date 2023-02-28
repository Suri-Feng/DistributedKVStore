package com.g3.CPEN431.project.Utils;// This file is cited from the utilities file provided in A01
// https://drive.google.com/drive/folders/1f4QfW2B8lkQe9MLL-DM9vEqR6sG5xbkK

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
            //str += " ";
            buf.append( str );
        }
        return buf.toString().toUpperCase();
    }
}