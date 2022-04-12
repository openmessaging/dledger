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

package io.openmessaging.storage.dledger.util;

import io.openmessaging.storage.dledger.utils.IOUtils;
import java.io.File;
import java.util.UUID;

public class FileTestUtil {

    public static final String TEST_BASE = File.separator + "tmp" + File.separator + "dledgerteststore";

    public static String createTestDir() {
        return createTestDir(null);
    }

    public static String createTestDir(String prefix) {
        if (prefix == null) {
            prefix = "test";
        }
        String baseDir = TEST_BASE + File.separator + prefix + "-" + UUID.randomUUID().toString();
        final File file = new File(baseDir);
        if (file.exists()) {
            System.exit(1);
        }
        return baseDir;
    }

    public static void deleteFile(String fileName) {
        IOUtils.deleteFile(new File(fileName));
    }

    public static void deleteFile(File file) {
        IOUtils.deleteFile(file);
    }

}
