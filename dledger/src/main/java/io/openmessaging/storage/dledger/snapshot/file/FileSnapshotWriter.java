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

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.snapshot.SnapshotStatus;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class FileSnapshotWriter implements SnapshotWriter {

    private static Logger logger = LoggerFactory.getLogger(FileSnapshotWriter.class);

    private final String snapshotStorePath;
    private final FileSnapshotStore snapshotStore;
    private SnapshotMeta snapshotMeta;

    public FileSnapshotWriter(String snapshotStorePath, FileSnapshotStore snapshotStore) {
        this.snapshotStorePath = snapshotStorePath;
        this.snapshotStore = snapshotStore;
    }

    @Override
    public void save(SnapshotStatus status) throws IOException {
        int res = status.getCode();
        IOException ioe = null;
        do {
            if (res != SnapshotStatus.SUCCESS.getCode()) {
                break;
            }
            try {
                sync();
            } catch (IOException e) {
                logger.error("Unable to sync writer: {}", this.snapshotStorePath, e);
                res = SnapshotStatus.FAIL.getCode();
                ioe = e;
                break;
            }
            // Move temp to new
            long snapshotIdx = getSnapshotIndex();
            String tmpPath = this.snapshotStorePath;
            String officialPath = this.snapshotStore.getSnapshotStoreBaseDir() + File.separator + SnapshotManager.SNAPSHOT_DIR_PREFIX + snapshotIdx;
            try {
                IOUtils.atomicMvFile(new File(tmpPath), new File(officialPath));
            } catch (IOException e) {
                logger.error("Unable to move temp snapshot from {} to {}", tmpPath, officialPath);
                res = SnapshotStatus.FAIL.getCode();
                ioe = e;
                break;
            }
        } while (false);
        // If the writing process failed, delete the temp snapshot
        if (res != SnapshotStatus.SUCCESS.getCode()) {
            try {
                IOUtils.deleteFile(new File(this.snapshotStorePath));
            } catch (IOException e) {
                logger.error("Unable to delete temp snapshot: {}", this.snapshotStorePath, e);
                ioe = e;
            }
        }
        if (ioe != null) {
            throw ioe;
        }
    }

    private void sync() throws IOException {
        IOUtils.string2File(JSON.toJSONString(this.snapshotMeta), this.snapshotStorePath + File.separator + SnapshotManager.SNAPSHOT_META_FILE);
    }

    @Override
    public String getSnapshotStorePath() {
        return this.snapshotStorePath;
    }

    @Override
    public void setSnapshotMeta(SnapshotMeta snapshotMeta) {
        this.snapshotMeta = snapshotMeta;
    }

    public long getSnapshotIndex() {
        return this.snapshotMeta != null ? this.snapshotMeta.getLastIncludedIndex() : -1;
    }
}
