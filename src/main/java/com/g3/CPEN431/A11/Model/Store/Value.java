package com.g3.CPEN431.A11.Model.Store;

import com.google.protobuf.ByteString;

public class Value {
    int version;
    ByteString value;
    long W_TS;
    long R_TS;
    public Value(int version, ByteString value, long W_TS) {
        this.value = value;
        this.version = version;
        this.R_TS  = 0;
        this.W_TS = W_TS;
    }

    public ByteString getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    public long getW_TS() {
        return W_TS;
    }

    public long getR_TS() {
        return R_TS;
    }

    public void setR_TS(long r_TS) {
        R_TS = r_TS;
    }
}
