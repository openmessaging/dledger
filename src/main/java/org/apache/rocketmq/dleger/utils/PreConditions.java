package org.apache.rocketmq.dleger.utils;

import org.apache.rocketmq.dleger.exception.DLegerException;

public class PreConditions {

    public static void check(boolean expression, DLegerException.Code code, String message) throws DLegerException {
        PreConditions.check(expression, code, message, null);
    }

    public static void check(boolean expression, DLegerException.Code code, String message, String leaderId) throws DLegerException {
        if (!expression) {
            if (message == null) message = code.toString();
            throw new DLegerException(code, message, leaderId);
        }
    }
}
