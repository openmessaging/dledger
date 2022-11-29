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

package io.openmessaging.storage.dledger.snapshot.file;

import io.openmessaging.storage.dledger.utils.IOUtils;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotStore;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSnapshotStore implements SnapshotStore {

    private static Logger logger = LoggerFactory.getLogger(FileSnapshotStore.class);

    private final String snapshotStoreBaseDir;

    public FileSnapshotStore(String snapshotStoreBaseDir) {
        this.snapshotStoreBaseDir = snapshotStoreBaseDir;
        initStore();
    }

    private void initStore() {
        // Create snapshot storage if the statemachine is first-time registered
        File dir = new File(this.snapshotStoreBaseDir);
        try {
            IOUtils.mkDir(dir);
        } catch (IOException e) {
            logger.error("Unable to create snapshot storage directory {}", this.snapshotStoreBaseDir, e);
            throw new RuntimeException(e);
        }
        // Clean temp directory to remove existing dirty snapshots
        File tmpSnapshot = new File(this.snapshotStoreBaseDir + File.separator + SnapshotManager.SNAPSHOT_TEMP_DIR);
        if (tmpSnapshot.exists()) {
            try {
                IOUtils.deleteFile(tmpSnapshot);
            } catch (IOException e) {
                logger.error("Unable to clean temp snapshots {}", tmpSnapshot.getPath(), e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SnapshotWriter createSnapshotWriter() {
        // Delete temp snapshot
        String tmpSnapshotStorePath = this.snapshotStoreBaseDir + File.separator + SnapshotManager.SNAPSHOT_TEMP_DIR;
        if (new File(tmpSnapshotStorePath).exists()) {
            try {
                IOUtils.deleteFile(new File(tmpSnapshotStorePath));
            } catch (IOException e) {
                logger.error("Unable to delete temp snapshot: {}", tmpSnapshotStorePath, e);
                return null;
            }
        }
        // Create tmp directory for writing snapshots
        File dir = new File(tmpSnapshotStorePath);
        try {
            IOUtils.mkDir(dir);
        } catch (IOException e) {
            logger.error("Unable to create snapshot storage directory: " + tmpSnapshotStorePath, e);
            return null;
        }
        return new FileSnapshotWriter(tmpSnapshotStorePath, this);
    }

    @Override
    public SnapshotReader createSnapshotReader() {
        long lastSnapshotIndex = getLastSnapshotIdx();
        if (lastSnapshotIndex == -1) {
            logger.warn("No snapshot exists");
            return null;
        }
        String snapshotStorePath = this.snapshotStoreBaseDir + File.separator +
                SnapshotManager.SNAPSHOT_DIR_PREFIX + lastSnapshotIndex;
        return new FileSnapshotReader(snapshotStorePath);
    }

    private long getLastSnapshotIdx() {
        File[] snapshotFiles = new File(this.snapshotStoreBaseDir).listFiles();
        long lastSnapshotIdx = -1;
        if (snapshotFiles != null && snapshotFiles.length > 0) {
            for (File snapshotFile : snapshotFiles) {
                String fileName = snapshotFile.getName();
                if (!fileName.startsWith(SnapshotManager.SNAPSHOT_DIR_PREFIX)) {
                    continue;
                }
                lastSnapshotIdx = Math.max(Long.parseLong(fileName.substring(SnapshotManager.SNAPSHOT_DIR_PREFIX.length())), lastSnapshotIdx);
            }
        }
        return lastSnapshotIdx;
    }

    public String getSnapshotStoreBaseDir() {
        return snapshotStoreBaseDir;
    }
}
