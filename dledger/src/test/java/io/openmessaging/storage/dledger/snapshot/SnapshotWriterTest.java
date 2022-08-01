package io.openmessaging.storage.dledger.snapshot;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.snapshot.file.FileSnapshotStore;
import io.openmessaging.storage.dledger.snapshot.file.FileSnapshotWriter;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import io.openmessaging.storage.dledger.utils.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

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
