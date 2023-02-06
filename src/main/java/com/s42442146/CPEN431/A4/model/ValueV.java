package com.s42442146.CPEN431.A4.model;

import com.google.protobuf.ByteString;

public class ValueV {
    int version;
    ByteString value;

    public ValueV(int version, ByteString value) {
        this.value = value;
        this.version = version;
    }

    public ByteString getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }


}
