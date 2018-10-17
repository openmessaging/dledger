package org.apache.rocketmq.dleger.protocol;

import java.util.HashMap;
import java.util.Map;

public enum  DLegerResponseCode {

    UNKNOWN(-1, "Unknown", ""),
    SUCCESS(200, "Append", ""),
    TIMEOUT(400, "Get", ""),
    NOT_LEADER(401, "Vote", ""),
    REJECT_EXPIRED_TERM(402, "HeartBeat", ""),
    INTERNAL_ERROR(500, "Pull", ""),
    ILLEGAL_ERROR(501, "Push", "");

    private static Map<Integer, DLegerResponseCode> codeMap = new HashMap<>();

    static {
        for (DLegerResponseCode responseCode : DLegerResponseCode.values()) {
            codeMap.put(responseCode.code, responseCode);
        }
    }

    private int code;
    private String name;
    private String desc;

    DLegerResponseCode(int code, String name, String desc) {
        this.code = code;
        this.name = name;
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

    public String getName() {
        return name;
    }

    public String getDesc() {
        return desc;
    }

}
