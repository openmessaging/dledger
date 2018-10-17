package org.apache.rocketmq.dleger.protocol;

import java.util.HashMap;
import java.util.Map;

public enum  DLegerRequestCode {
    UNKNOWN(-1, "Unknown", ""),
    APPEND(50001, "Append", ""),
    GET(50002, "Get", ""),
    VOTE(51001, "Vote", ""),
    HEART_BEAT(51002, "HeartBeat", ""),
    PULL(51003, "Pull", ""),
    PUSH(51004, "Push", "");

    private static Map<Integer, DLegerRequestCode> codeMap = new HashMap<>();

    static {
        for (DLegerRequestCode requestCode: DLegerRequestCode.values()) {
            codeMap.put(requestCode.code, requestCode);
        }
    }

    private int code;
    private String name;
    private String desc;

    DLegerRequestCode(int code, String name, String desc) {
        this.code = code;
        this.name = name;
        this.desc = desc;
    }

    public static DLegerRequestCode valueOf(int code) {
        DLegerRequestCode tmp = codeMap.get(code);
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
