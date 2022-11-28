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
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.snapshot.SnapshotStatus;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.snapshot.file.FileSnapshotStore;
import io.openmessaging.storage.dledger.snapshot.file.FileSnapshotWriter;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SnapshotWriterTest {

    @Test
    public void testWriterSave() throws IOException {
        final long lastSnapshotIndex = 10;
        SnapshotWriter writer = new FileSnapshotWriter(FileTestUtil.TEST_BASE + File.separator + "tmp", new FileSnapshotStore(FileTestUtil.TEST_BASE));

        SnapshotMeta snapshotMeta = new SnapshotMeta(lastSnapshotIndex, 0);
        writer.setSnapshotMeta(snapshotMeta);
        writer.save(SnapshotStatus.SUCCESS);

        String testDir = FileTestUtil.TEST_BASE + File.separator + SnapshotManager.SNAPSHOT_DIR_PREFIX + lastSnapshotIndex;
        try {
            Assertions.assertEquals(IOUtils.file2String(testDir + File.separator +
                    SnapshotManager.SNAPSHOT_META_FILE), JSON.toJSONString(snapshotMeta));
        } finally {
            IOUtils.deleteFile(new File(testDir));
        }
    }
}
