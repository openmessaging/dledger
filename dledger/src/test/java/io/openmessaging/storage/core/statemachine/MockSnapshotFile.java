/*
 * Copyright 2017-2022 The DLedger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.core.statemachine;

import io.openmessaging.storage.dledger.common.utils.IOUtils;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockSnapshotFile {

    private static Logger logger = LoggerFactory.getLogger(MockSnapshotFile.class);

    private final String snapshotStorePath;

    public MockSnapshotFile(String snapshotStorePath) {
        this.snapshotStorePath = snapshotStorePath;
    }

    public boolean save(final long value) {
        try {
            IOUtils.string2File(String.valueOf(value), snapshotStorePath);
            return true;
        } catch (IOException e) {
            logger.error("Unable to save snapshot data", e);
            return false;
        }
    }

    public long load() throws IOException {
        String str = IOUtils.file2String(new File(snapshotStorePath));
        if (str != null && str.length() != 0) {
            return Long.parseLong(str);
        } else {
            throw new IOException("Unable to load snapshot data from " + snapshotStorePath);
        }
    }
}