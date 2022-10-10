package io.openmessaging.storage.dledger.statemachine;

import io.openmessaging.storage.dledger.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

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