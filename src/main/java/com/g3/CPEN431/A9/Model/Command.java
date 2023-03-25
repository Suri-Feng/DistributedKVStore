package com.g3.CPEN431.A9.Model;

import java.util.Arrays;
import java.util.Optional;

public enum Command {
    PUT (0x01),
    GET (0x02),
    REMOVE (0x03),
    SHUTDOWN (0x04),
    WIPE_OUT (0x05),
    IS_ALIVE (0x06),
    GET_PID (0x07),
    GET_MEMBERSHIP_COUNT (0x08),
    HEARTBEAT(20),
    KEY_TRANSFER(21),
    SUCCESSOR_NOTIFY(22),
    BACKUP_WRITE(23),
    BACKUP_REM(24),
    BACKUP_ACK(25);

    private final int code;

    public static Optional<Command> findCommand (int code) {
        return Arrays.stream(values())
                .filter(c -> c.code == code)
                .findFirst();
    }

    Command (int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
