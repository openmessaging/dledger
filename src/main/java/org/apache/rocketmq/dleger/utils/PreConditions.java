package org.apache.rocketmq.dleger.utils;

import org.apache.rocketmq.dleger.exception.DLegerException;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;

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
