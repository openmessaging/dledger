package io.openmessaging.storage.dledger.snapshot;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.snapshot.file.FileSnapshotStore;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import io.openmessaging.storage.dledger.utils.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class SnapshotStoreTest {

    @Test
    public void testCreateReaderAndWriter() throws IOException {
        final long lastSnapshotIndex = 10;
        try {
            FileSnapshotStore writerStore = new FileSnapshotStore(FileTestUtil.TEST_BASE);
            SnapshotWriter writer = writerStore.createSnapshotWriter();
            Assertions.assertNotNull(writer);
            SnapshotMeta writerMeta = new SnapshotMeta(lastSnapshotIndex, 0);
            writer.setSnapshotMeta(writerMeta);
            writer.save(SnapshotStatus.SUCCESS);

            FileSnapshotStore readerStore = new FileSnapshotStore(FileTestUtil.TEST_BASE);
            SnapshotReader reader = readerStore.createSnapshotReader();
            Assertions.assertNotNull(reader);
            SnapshotMeta readerMeta = reader.load();
            Assertions.assertEquals(writerMeta.toString(), readerMeta.toString());
        } finally {
            IOUtils.deleteFile(new File(FileTestUtil.TEST_BASE + File.separator + SnapshotManager.SNAPSHOT_DIR_PREFIX + lastSnapshotIndex));
        }
    }
}
