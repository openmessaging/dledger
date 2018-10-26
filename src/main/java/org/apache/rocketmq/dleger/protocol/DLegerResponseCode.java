package org.apache.rocketmq.dleger.protocol;

import java.util.HashMap;
import java.util.Map;

public enum  DLegerResponseCode {

    UNKNOWN(-1, ""),
    SUCCESS(200, ""),
    TIMEOUT(300, ""),
    NETWORK_ERROR(302, ""),
    UNKNOWN_MEMBER(400, ""),
    UNEXPECTED_MEMBER(401, ""),
    EXPIRED_TERM(402, ""),
    NOT_LEADER(403, ""),
    NOT_FOLLOWER(404, ""),
    INCONSISTENT_STATE(405, ""),
    INCONSISTENT_TERM(406, ""),
    INCONSISTENT_INDEX(407, ""),
    INCONSISTENT_LEADER(408, ""),
    INDEX_OUT_OF_RANGE(409, ""),
    UNEXPECTED_ARGUMENT(410, ""),
    REPEATED_REQUEST(411, ""),
    REPEATED_PUSH(412, ""),
    DISK_ERROR(413, ""),
    NOT_READY(414, ""),
    INTERNAL_ERROR(500, ""),
    TERM_CHANGED(501, ""),
    WAIT_QUORUM_ACK_TIMEOUT(502, ""),
    LEADER_PENDING_FULL(503, ""),
    ILLEGAL_MEMBER_STATE(504, "");

    private static Map<Integer, DLegerResponseCode> codeMap = new HashMap<>();

    static {
        for (DLegerResponseCode responseCode : DLegerResponseCode.values()) {
            codeMap.put(responseCode.code, responseCode);
        }
    }

    private int code;
    private String desc;

    DLegerResponseCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static DLegerResponseCode valueOf(int code) {
        DLegerResponseCode tmp = codeMap.get(code);
        if (tmp != null) {
            return tmp;
        } else {
            return UNKNOWN;
        }

    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        return String.format("[code=%d,name=%s,desc=%s]", code, name(), desc);
    }
}
