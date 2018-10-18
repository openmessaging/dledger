package org.apache.rocketmq.dleger.utils;

import org.apache.rocketmq.dleger.exception.DLegerException;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;

public class PreConditions {

    public static void check(boolean expression, DLegerResponseCode code) throws DLegerException {
        check(expression, code, null);
    }

    public static void check(boolean expression, DLegerResponseCode code, String message) throws DLegerException {
        if (!expression) {
            if (message == null) {
                message = code.toString();
            }  else {
                message = code.toString() + " " + message;
            }
            throw new DLegerException(code, message);
        }
    }

    public static void check(boolean expression, DLegerResponseCode code, String format, Object... args) throws DLegerException {
        check(expression, code, String.format(format, args));
    }
}
