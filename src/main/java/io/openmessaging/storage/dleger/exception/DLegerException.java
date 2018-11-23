package io.openmessaging.storage.dleger.exception;

import io.openmessaging.storage.dleger.protocol.DLegerResponseCode;

public class DLegerException extends RuntimeException {

    private final DLegerResponseCode code;
    public DLegerException(DLegerResponseCode code, String message) {
        super(message);
        this.code = code;
    }

    public DLegerException(DLegerResponseCode code, String format, Object... args) {
        super(String.format(format, args));
        this.code = code;
    }

    public DLegerResponseCode getCode() {
        return code;
    }
}
