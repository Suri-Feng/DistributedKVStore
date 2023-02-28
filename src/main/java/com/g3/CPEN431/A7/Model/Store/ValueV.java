package com.g3.CPEN431.A7.Model.Store;

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
