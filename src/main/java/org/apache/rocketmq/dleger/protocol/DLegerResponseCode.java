package org.apache.rocketmq.dleger.protocol;

import java.util.HashMap;
import java.util.Map;

public enum  DLegerResponseCode {

    UNKNOWN(-1, ""),
    SUCCESS(200, ""),
    TIMEOUT(300, ""),
    METADATA_ERROR(301, ""),
    NETWORK_ERROR(302, ""),
    UNSUPPORTED(303, ""),
    UNKNOWN_GROUP(400, ""),
    UNKNOWN_MEMBER(401, ""),
    UNEXPECTED_MEMBER(402, ""),
    EXPIRED_TERM(403, ""),
    NOT_LEADER(404, ""),
    NOT_FOLLOWER(405, ""),
    INCONSISTENT_STATE(406, ""),
    INCONSISTENT_TERM(407, ""),
    INCONSISTENT_INDEX(408, ""),
    INCONSISTENT_LEADER(409, ""),
    INDEX_OUT_OF_RANGE(410, ""),
    UNEXPECTED_ARGUMENT(411, ""),
    REPEATED_REQUEST(412, ""),
    REPEATED_PUSH(413, ""),
    DISK_ERROR(414, ""),
    DISK_FULL(415, ""),
    TERM_NOT_READY(416, ""),
    INTERNAL_ERROR(500, ""),
    TERM_CHANGED(501, ""),
    WAIT_QUORUM_ACK_TIMEOUT(502, ""),
    LEADER_PENDING_FULL(503, ""),
    ILLEGAL_MEMBER_STATE(504, ""),
    LEADER_NOT_READY(505, "");

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
