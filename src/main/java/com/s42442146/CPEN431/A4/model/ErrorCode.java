package com.s42442146.CPEN431.A4.model;

public enum ErrorCode {
    SUCCESSFUL (0x00),
    NONEXISTENT_KEY (0x01),
    OUT_OF_SPACE (0x02),
    OVERLOAD (0x03),
    INTERNAL_FAILURE (0x04),
    UNKNOWN_COMMAND(0x05),
    INVALID_KEY (0x06),
    INVALID_VALUE (0x07);

    private int code;

    public int getCode() {
        return code;
    }
    ErrorCode (int code) {
        this.code = code;
    }
}
