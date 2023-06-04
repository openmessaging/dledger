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
import io.openmessaging.storage.dledger.snapshot.DownloadSnapshot;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.utils.IOUtils;
import java.io.FileInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class FileSnapshotReader implements SnapshotReader {

    private static Logger logger = LoggerFactory.getLogger(FileSnapshotReader.class);

    private final String snapshotStorePath;
    private SnapshotMeta snapshotMeta;

    public FileSnapshotReader(String snapshotStorePath) {
        this.snapshotStorePath = snapshotStorePath;
    }

    @Override
    public SnapshotMeta load() throws IOException {
        SnapshotMeta snapshotMetaFromJSON = JSON.parseObject(IOUtils.file2String(this.snapshotStorePath +
                File.separator + SnapshotManager.SNAPSHOT_META_FILE), SnapshotMeta.class);
        if (snapshotMetaFromJSON == null) {
            return null;
        }
        this.snapshotMeta = snapshotMetaFromJSON;
        return snapshotMeta;
    }

    @Override
    public SnapshotMeta getSnapshotMeta() {
        return this.snapshotMeta != null ? this.snapshotMeta : null;
    }

    @Override
    public String getSnapshotStorePath() {
        return this.snapshotStorePath;
    }

    @Override
    public DownloadSnapshot generateDownloadSnapshot() {
        try {
            load();
        } catch (Exception e) {
            logger.error("load snapshot metadata error", e);
            return null;
        }
        File dataFile = new File(this.snapshotStorePath + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);
        if (!dataFile.exists()) {
            logger.error("snapshot data file not exist, {}", dataFile.getAbsolutePath());
            return null;
        }
        DownloadSnapshot downloadSnapshot = new DownloadSnapshot();
        downloadSnapshot.setMeta(this.snapshotMeta);
        byte[] data = new byte[(int) dataFile.length()];
        try {
            FileInputStream inputStream = new FileInputStream(dataFile);
            inputStream.read(data);
            inputStream.close();
            downloadSnapshot.setData(data);
        } catch (Exception e) {
            logger.error("load snapshot data file error", e);
            return null;
        }
        return downloadSnapshot;
    }
}
