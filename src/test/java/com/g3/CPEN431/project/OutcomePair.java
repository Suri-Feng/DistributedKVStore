package com.g3.CPEN431.project;

import com.google.protobuf.ByteString;

public class OutcomePair {
    enum Status {
        SUCCESS,
        KEYNOTFOUND,
        TIMEOUT
    }

    private Status status;
    private String val;

    public OutcomePair() {

    }
    public OutcomePair(Status status, String value) {
        this.status = status;
        this.val = value;
    }

    OutcomePair getOutcome() {
        return this;
    }

    void setOutcome(Status status, String value) {
        this.status = status;
        this.val = value;
    }

    String getStatus() {
        return status.toString();
    }

    String getValue() {
        return val;
    }
}
