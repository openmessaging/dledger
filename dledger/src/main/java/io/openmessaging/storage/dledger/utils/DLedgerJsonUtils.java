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

package io.openmessaging.storage.dledger.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.JSONWriter;
import java.lang.invoke.MethodHandles;

public final class DLedgerJsonUtils {

    static {
        // Fastjson2 2.0.63 may read IMPL_LOOKUP before MethodHandles.Lookup is initialized on JDK 8.
        MethodHandles.lookup();
    }

    private DLedgerJsonUtils() {
    }

    public static void ensureInitialized() {
        // Calling this method triggers the JDK 8 compatibility initialization above.
    }

    public static byte[] toJsonBytes(Object object) {
        return JSON.toJSONBytes(object, JSONWriter.Feature.WriteByteArrayAsBase64);
    }

    public static String toJsonString(Object object) {
        return JSON.toJSONString(object, JSONWriter.Feature.WriteByteArrayAsBase64);
    }

    public static <T> T parseObject(byte[] bytes, Class<T> clazz) {
        return JSON.parseObject(bytes, clazz, JSONReader.Feature.Base64StringAsByteArray);
    }

    public static <T> T parseObject(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz, JSONReader.Feature.Base64StringAsByteArray);
    }
}
