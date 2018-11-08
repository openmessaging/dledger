package org.apache.rocketmq.dleger.protocol;

import java.util.HashMap;
import java.util.Map;

public enum  DLegerRequestCode {
    UNKNOWN(-1, ""),
    METADATA(50000, ""),
    APPEND(50001, ""),
    GET(50002, ""),
    VOTE(51001, ""),
    HEART_BEAT(51002, ""),
    PULL(51003, ""),
    PUSH(51004, "");

    private static Map<Integer, DLegerRequestCode> codeMap = new HashMap<>();

    static {
        for (DLegerRequestCode requestCode: DLegerRequestCode.values()) {
            codeMap.put(requestCode.code, requestCode);
        }
    }

    private int code;
    private String desc;

    DLegerRequestCode(int code, String desc) {
        this.code = code;
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


    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        return String.format("[code=%d,name=%s,desc=%s]", code, name(), desc);
    }

}
