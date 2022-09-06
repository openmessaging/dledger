/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.protocol;

import java.util.HashMap;
import java.util.Map;

public enum DLedgerResponseCode {

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
    FALL_BEHIND_TOO_MUCH(417, ""),
    TAKE_LEADERSHIP_FAILED(418, ""),
    INDEX_LESS_THAN_LOCAL_BEGIN(419, ""),
    REQUEST_WITH_EMPTY_BODYS(420, ""),
    INTERNAL_ERROR(500, ""),
    TERM_CHANGED(501, ""),
    WAIT_QUORUM_ACK_TIMEOUT(502, ""),
    LEADER_PENDING_FULL(503, ""),
    ILLEGAL_MEMBER_STATE(504, ""),
    LEADER_NOT_READY(505, ""),
    LEADER_TRANSFERRING(506, "");

    private static Map<Integer, DLedgerResponseCode> codeMap = new HashMap<>();

    static {
        for (DLedgerResponseCode responseCode : DLedgerResponseCode.values()) {
            codeMap.put(responseCode.code, responseCode);
        }
    }

    private int code;
    private String desc;

    DLedgerResponseCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static DLedgerResponseCode valueOf(int code) {
        DLedgerResponseCode tmp = codeMap.get(code);
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
