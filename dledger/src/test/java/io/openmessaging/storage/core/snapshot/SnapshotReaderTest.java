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

package io.openmessaging.storage.core.snapshot;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.core.util.FileTestUtil;
import io.openmessaging.storage.dledger.common.utils.IOUtils;
import io.openmessaging.storage.dledger.core.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.core.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.core.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.core.snapshot.file.FileSnapshotReader;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SnapshotReaderTest {

    @Test
    public void testReaderLoad() throws IOException {
        String metaFilePath = FileTestUtil.TEST_BASE + File.separator + SnapshotManager.SNAPSHOT_META_FILE;
        try {
            SnapshotMeta snapshotMeta = new SnapshotMeta(10, 0);
            IOUtils.string2File(JSON.toJSONString(snapshotMeta), metaFilePath);

            SnapshotReader reader = new FileSnapshotReader(FileTestUtil.TEST_BASE);
            Assertions.assertNull(reader.getSnapshotMeta());
            Assertions.assertEquals(reader.getSnapshotStorePath(), FileTestUtil.TEST_BASE);
            Assertions.assertEquals(reader.load().toString(), snapshotMeta.toString());
        } finally {
            IOUtils.deleteFile(new File(metaFilePath));
        }
    }
}
