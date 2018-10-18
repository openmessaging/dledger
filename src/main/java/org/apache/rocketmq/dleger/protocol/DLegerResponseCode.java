package org.apache.rocketmq.dleger.protocol;

import java.util.HashMap;
import java.util.Map;

public enum  DLegerResponseCode {

    UNKNOWN(-1, ""),
    SUCCESS(200, ""),
    TIMEOUT(300, ""),
    REJECT_EXPIRED_TERM(400, ""),
    NOT_LEADER(401, ""),
    NOT_FOLLOWER(402, ""),
    INCONSISTENT_STATE(403, ""),
    INCONSISTENT_TERM(404, ""),
    INCONSISTENT_INDEX(405, ""),
    INCONSISTENT_LEADER(406, ""),
    INDEX_OUT_OF_RANGE(407, ""),
    ILLEGAL_ERROR(410, ""),
    DISK_ERROR(408, ""),
    INTERNAL_ERROR(500, "");

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
