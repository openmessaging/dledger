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

package io.openmessaging.storage.dledger.snapshot.file;

import io.openmessaging.storage.dledger.snapshot.DownloadSnapshot;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotStatus;
import io.openmessaging.storage.dledger.snapshot.SnapshotStore;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.utils.IOUtils;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class FileSnapshotStore implements SnapshotStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSnapshotStore.class);

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
            LOGGER.error("Unable to create snapshot storage directory {}", this.snapshotStoreBaseDir, e);
            throw new RuntimeException(e);
        }
        // Clean temp directory to remove existing dirty snapshots
        File tmpSnapshot = new File(this.snapshotStoreBaseDir + File.separator + SnapshotManager.SNAPSHOT_TEMP_DIR);
        if (tmpSnapshot.exists()) {
            try {
                IOUtils.deleteFile(tmpSnapshot);
            } catch (IOException e) {
                LOGGER.error("Unable to clean temp snapshots {}", tmpSnapshot.getPath(), e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SnapshotWriter createSnapshotWriter() {
        return createSnapshotWriter(this.snapshotStoreBaseDir + File.separator + SnapshotManager.SNAPSHOT_TEMP_DIR);
    }

    private SnapshotWriter createSnapshotWriter(String snapshotStorePath) {
        if (new File(snapshotStorePath).exists()) {
            try {
                IOUtils.deleteFile(new File(snapshotStorePath));
            } catch (IOException e) {
                LOGGER.error("Unable to delete temp snapshot: {}", snapshotStorePath, e);
                return null;
            }
        }
        // Create tmp directory for writing snapshots
        File dir = new File(snapshotStorePath);
        try {
            IOUtils.mkDir(dir);
        } catch (IOException e) {
            LOGGER.error("Unable to create snapshot storage directory: " + snapshotStorePath, e);
            return null;
        }
        return new FileSnapshotWriter(snapshotStorePath, this);
    }

    @Override
    public SnapshotReader createSnapshotReader() {
        long lastSnapshotIndex = getLastSnapshotIdx();
        if (lastSnapshotIndex == -1) {
            LOGGER.warn("No snapshot exists");
            return null;
        }
        String snapshotStorePath = this.snapshotStoreBaseDir + File.separator +
            SnapshotManager.SNAPSHOT_DIR_PREFIX + lastSnapshotIndex;
        return new FileSnapshotReader(snapshotStorePath);
    }

    @Override
    public boolean downloadSnapshot(DownloadSnapshot downloadSnapshot) {
        // clear temp install snapshot dir
        String installTmpDir = this.snapshotStoreBaseDir + File.separator + SnapshotManager.SNAPSHOT_INSTALL_TEMP_DIR;
        File installTmpDirFile = new File(installTmpDir);
        if (installTmpDirFile.exists()) {
            try {
                IOUtils.deleteFile(installTmpDirFile);
            } catch (IOException e) {
                LOGGER.error("Unable to delete temp install snapshot: {}", installTmpDir, e);
                return false;
            }
        }
        // create temp install snapshot dir
        try {
            IOUtils.mkDir(installTmpDirFile);
        } catch (IOException e) {
            LOGGER.error("Unable to create temp install snapshot dir: {}", installTmpDir, e);
            return false;
        }
        // write meta and data to temp install snapshot dir and then move it to snapshot store dir
        try {
            SnapshotWriter writer = createSnapshotWriter(installTmpDir);
            if (writer == null) {
                LOGGER.error("Unable to create snapshot writer for install snapshot: {}", downloadSnapshot);
                return false;
            }
            writer.setSnapshotMeta(downloadSnapshot.getMeta());
            FileOutputStream fileOutputStream = new FileOutputStream(writer.getSnapshotStorePath() + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);
            fileOutputStream.write(downloadSnapshot.getData());
            fileOutputStream.flush();
            fileOutputStream.close();
            writer.save(SnapshotStatus.SUCCESS);
            return true;
        } catch (Exception e) {
            LOGGER.error("Unable to write snapshot: {} data to install snapshot", downloadSnapshot, e);
            return false;
        }
    }

    @Override
    public void deleteExpiredSnapshot(long maxReservedSnapshotNum) {
        // Remove the oldest snapshot
        List<File> realSnapshotFiles = getSnapshotFiles().stream().sorted((o1, o2) -> {
                long idx1 = Long.parseLong(o1.getName().substring(SnapshotManager.SNAPSHOT_DIR_PREFIX.length()));
                long idx2 = Long.parseLong(o2.getName().substring(SnapshotManager.SNAPSHOT_DIR_PREFIX.length()));
                return Long.compare(idx1, idx2);
            }
            ).collect(Collectors.toList());
        if (realSnapshotFiles.size() <= maxReservedSnapshotNum) {
            return;
        }
        realSnapshotFiles.stream().limit(realSnapshotFiles.size() - maxReservedSnapshotNum).forEach(file -> {
            try {
                IOUtils.deleteFile(file);
                LOGGER.info("Delete expired snapshot: {}", file.getPath());
            } catch (IOException e) {
                LOGGER.error("Unable to remove expired snapshot: {}", file.getPath(), e);
            }
        });
    }

    @Override
    public long getSnapshotNum() {
        return getSnapshotFiles().size();
    }

    private long getLastSnapshotIdx() {
        Optional<File> optionalFile = getSnapshotFiles().stream().min((o1, o2) -> {
                long idx1 = Long.parseLong(o1.getName().substring(SnapshotManager.SNAPSHOT_DIR_PREFIX.length()));
                long idx2 = Long.parseLong(o2.getName().substring(SnapshotManager.SNAPSHOT_DIR_PREFIX.length()));
                return Long.compare(idx2, idx1);
            }
        );
        long index = -1;
        if (optionalFile.isPresent()) {
            File file = optionalFile.get();
            index = Long.parseLong(file.getName().substring(SnapshotManager.SNAPSHOT_DIR_PREFIX.length()));
        }
        return index;
    }

    private List<File> getSnapshotFiles() {
        File[] snapshotFiles = new File(this.snapshotStoreBaseDir).listFiles();
        if (snapshotFiles == null || snapshotFiles.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(snapshotFiles).filter(file -> file.getName().startsWith(SnapshotManager.SNAPSHOT_DIR_PREFIX)).collect(Collectors.toList());
    }

    public String getSnapshotStoreBaseDir() {
        return snapshotStoreBaseDir;
    }
}
