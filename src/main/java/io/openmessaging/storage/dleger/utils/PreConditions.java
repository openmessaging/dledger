package io.openmessaging.storage.dleger.utils;

import io.openmessaging.storage.dleger.protocol.DLegerResponseCode;
import io.openmessaging.storage.dleger.exception.DLegerException;

public class PreConditions {

    public static void check(boolean expression, DLegerResponseCode code) throws DLegerException {
        check(expression, code, null);
    }

    public static void check(boolean expression, DLegerResponseCode code, String message) throws DLegerException {
        if (!expression) {
            message = message == null ? code.toString()
                : code.toString() + " " + message;
            throw new DLegerException(code, message);
        }
    }

    public static void check(boolean expression, DLegerResponseCode code, String format, Object... args) throws DLegerException {
        if (!expression) {
            String message = code.toString() + " " + String.format(format, args);
            throw new DLegerException(code, message);
        }
    }
}
