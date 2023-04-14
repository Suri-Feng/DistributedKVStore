package com.g3.CPEN431.A11.Model.Store;

import com.google.protobuf.ByteString;

public class Value {
    int version;
    ByteString value;
    long R_TS;
    public Value(int version, ByteString value, long R_TS) {
        this.value = value;
        this.version = version;
        this.R_TS = R_TS;
    }

    public ByteString getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    public long getR_TS() {
        return R_TS;
    }
}
