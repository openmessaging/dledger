package org.apache.rocketmq.dleger.exception;

import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;

public class DLegerException extends RuntimeException {

    private final DLegerResponseCode code;
    public DLegerException(DLegerResponseCode code, String message) {
        super(message);
        this.code = code;
    }

    public DLegerException(DLegerResponseCode code, String format, String... args) {
        super(String.format(format, args));
        this.code = code;
    }

    public DLegerResponseCode getCode() {
        return code;
    }
}
