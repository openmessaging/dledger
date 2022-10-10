package io.openmessaging.storage.dledger.snapshot;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.snapshot.file.FileSnapshotReader;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import io.openmessaging.storage.dledger.utils.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

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
