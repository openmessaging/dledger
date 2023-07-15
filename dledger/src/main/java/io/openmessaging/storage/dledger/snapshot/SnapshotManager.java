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

package io.openmessaging.storage.dledger.snapshot;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.common.Closure;
import io.openmessaging.storage.dledger.common.NamedThreadFactory;
import io.openmessaging.storage.dledger.common.Status;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.snapshot.file.FileSnapshotStore;
import io.openmessaging.storage.dledger.snapshot.hook.LoadSnapshotHook;
import io.openmessaging.storage.dledger.snapshot.hook.SaveSnapshotHook;
import io.openmessaging.storage.dledger.utils.IOUtils;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * <h>snapshot dir tree (here is an example)</h>
 * <pre>
 *   snapshot
 *     - tmp  (saving snapshot now, but has not been renamed to a snapshot dir)
 *       - snapshot_meta  (index=39, term=1)
 *       - data  (statemachine data util index=39(included))
 *
 *     - snapshot_13  (means snapshot which has statemachine data util index=13(included))
 *       - snapshot_meta  (index=13, term=1)
 *       - data  (statemachine data util index=13(included))
 *
 *     - snapshot_26  (means snapshot which has statemachine data util index=26(included))
 *       - snapshot_meta  (index=26, term=1)
 *       - data
 *
 *     - install_tmp  (downloaded snapshot from leader, but has not been renamed to a snapshot dir)
 *       - snapshot_meta
 *       - data
 *
 * </pre>
 */
public class SnapshotManager {

    private static Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    public static final String SNAPSHOT_META_FILE = "snapshot_meta";
    public static final String SNAPSHOT_DATA_FILE = "data";
    public static final String SNAPSHOT_DIR_PREFIX = "snapshot_";
    public static final String SNAPSHOT_TEMP_DIR = "tmp";
    public static final String SNAPSHOT_INSTALL_TEMP_DIR = "install_tmp";

    private DLedgerServer dLedgerServer;

    private DLedgerConfig dLedgerConfig;
    private volatile long lastSnapshotIndex = -1;
    private volatile long lastSnapshotTerm = -1;
    private final SnapshotStore snapshotStore;

    private final MemberState memberState;
    private volatile boolean savingSnapshot;
    private volatile boolean loadingSnapshot;

    private final ScheduledExecutorService scheduledExecutorService;

    public SnapshotManager(DLedgerServer dLedgerServer) {
        this.dLedgerServer = dLedgerServer;
        this.dLedgerConfig = this.dLedgerServer.getDLedgerConfig();
        this.memberState = this.dLedgerServer.getMemberState();
        this.snapshotStore = new FileSnapshotStore(this.dLedgerServer.getDLedgerConfig().getSnapshotStoreBaseDir());
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("SnapshotManager-EntriesResetService", true));
    }

    public boolean isSavingSnapshot() {
        return savingSnapshot;
    }

    public boolean isLoadingSnapshot() {
        return loadingSnapshot;
    }

    private class SaveSnapshotAfterHook implements SaveSnapshotHook {

        SnapshotWriter writer;
        SnapshotMeta snapshotMeta;

        public SaveSnapshotAfterHook(SnapshotWriter writer) {
            this.writer = writer;
        }

        @Override
        public void doCallBack(SnapshotStatus status) {
            saveSnapshotAfter(writer, snapshotMeta, status);
        }

        @Override
        public void registerSnapshotMeta(SnapshotMeta snapshotMeta) {
            this.snapshotMeta = snapshotMeta;
            this.writer.setSnapshotMeta(snapshotMeta);
        }

        @Override
        public SnapshotWriter getSnapshotWriter() {
            return this.writer;
        }

    }

    private class LoadSnapshotAfterHook implements LoadSnapshotHook {

        SnapshotReader reader;
        SnapshotMeta snapshotMeta;

        Closure closure;

        public LoadSnapshotAfterHook(SnapshotReader reader, Closure closure) {
            this.reader = reader;
            this.closure = closure;
        }

        @Override
        public void doCallBack(SnapshotStatus status) {
            loadSnapshotAfter(reader, snapshotMeta, status, closure);
        }

        @Override
        public void registerSnapshotMeta(SnapshotMeta snapshotMeta) {
            this.snapshotMeta = snapshotMeta;
        }

        @Override
        public SnapshotReader getSnapshotReader() {
            return this.reader;
        }
    }

    public void saveSnapshot() {
        // Check if still saving other snapshots
        if (this.savingSnapshot) {
            return;
        }
        // Check if applied index reaching the snapshot threshold
        if (this.memberState.getAppliedIndex() - this.lastSnapshotIndex < this.dLedgerServer.getDLedgerConfig().getSnapshotThreshold()) {
            return;
        }
        // Create snapshot writer
        SnapshotWriter writer = this.snapshotStore.createSnapshotWriter();
        if (writer == null) {
            return;
        }
        // Start saving snapshot
        this.savingSnapshot = true;
        SaveSnapshotAfterHook saveSnapshotAfter = new SaveSnapshotAfterHook(writer);
        if (!this.dLedgerServer.getFsmCaller().onSnapshotSave(saveSnapshotAfter)) {
            logger.error("Unable to call statemachine onSnapshotSave");
            saveSnapshotAfter.doCallBack(SnapshotStatus.FAIL);
        }
    }

    private void saveSnapshotAfter(SnapshotWriter writer, SnapshotMeta snapshotMeta, SnapshotStatus status) {
        int res = status.getCode();
        // Update snapshot meta
        if (res == SnapshotStatus.SUCCESS.getCode()) {
            writer.setSnapshotMeta(snapshotMeta);
        }
        // Write snapshot meta into files and close snapshot writer
        try {
            writer.save(status);
        } catch (IOException e) {
            logger.error("Unable to close snapshot writer", e);
            res = SnapshotStatus.FAIL.getCode();
        }
        if (res == SnapshotStatus.SUCCESS.getCode()) {
            this.lastSnapshotIndex = snapshotMeta.getLastIncludedIndex();
            this.lastSnapshotTerm = snapshotMeta.getLastIncludedTerm();
            logger.info("Snapshot {} saved successfully", snapshotMeta);
            resetSnapshotAfterSave(lastSnapshotIndex, lastSnapshotTerm);
        } else {
            logger.error("Unable to save snapshot, res: {}", res);
        }
        this.savingSnapshot = false;
    }

    private void resetSnapshotAfterSave(long lastIncludedIndex, long lastIncludedTerm) {
        switch (this.dLedgerConfig.getSnapshotEntryResetStrategy()) {
            case RESET_ALL_SYNC:
                truncatePrefix(lastIncludedIndex, lastIncludedTerm);
                break;
            case RESET_ALL_ASYNC:
                CompletableFuture.runAsync(() -> {
                    truncatePrefix(lastIncludedIndex, lastIncludedTerm);
                });
                break;
            case RESET_ALL_LATER:
                this.scheduledExecutorService.schedule(() -> {
                    truncatePrefix(lastIncludedIndex, lastIncludedTerm);
                }, this.dLedgerConfig.getResetSnapshotEntriesDelayTime(), TimeUnit.MILLISECONDS);
                break;
            case RESET_BUT_KEEP_SOME_SYNC:
                truncatePrefix(lastIncludedIndex - dLedgerConfig.getResetSnapshotEntriesButKeepLastEntriesNum(), lastIncludedTerm);
                break;
            case RESET_BUT_KEEP_SOME_ASYNC:
                CompletableFuture.runAsync(() -> {
                    truncatePrefix(lastIncludedIndex - dLedgerConfig.getResetSnapshotEntriesButKeepLastEntriesNum(), lastIncludedTerm);
                });
                break;
            case RESET_BUT_KEEP_SOME_LATER:
                this.scheduledExecutorService.schedule(() -> {
                    truncatePrefix(lastIncludedIndex - dLedgerConfig.getResetSnapshotEntriesButKeepLastEntriesNum(), lastIncludedTerm);
                }, this.dLedgerConfig.getResetSnapshotEntriesDelayTime(), TimeUnit.MILLISECONDS);
                break;
            default:
                logger.error("Unknown reset strategy {}", this.dLedgerConfig.getSnapshotEntryResetStrategy());
                break;
        }
    }

    private void truncatePrefix(long lastIncludedIndex, long lastIncludedTerm) {
        deleteExpiredSnapshot();
        this.dLedgerServer.getDLedgerStore().reset(lastIncludedIndex, lastIncludedTerm);
    }

    private void deleteExpiredSnapshot() {
        // Remove the oldest snapshot
        DLedgerConfig config = dLedgerServer.getDLedgerConfig();
        File[] snapshotFiles = new File(config.getSnapshotStoreBaseDir()).listFiles();
        if (snapshotFiles != null && snapshotFiles.length > config.getMaxSnapshotReservedNum()) {
            long minSnapshotIdx = Long.MAX_VALUE;
            for (File file : snapshotFiles) {
                String fileName = file.getName();
                if (!fileName.startsWith(SnapshotManager.SNAPSHOT_DIR_PREFIX)) {
                    continue;
                }
                minSnapshotIdx = Math.min(Long.parseLong(fileName.substring(SnapshotManager.SNAPSHOT_DIR_PREFIX.length())), minSnapshotIdx);
            }
            String deleteFilePath = config.getSnapshotStoreBaseDir() + File.separator + SnapshotManager.SNAPSHOT_DIR_PREFIX + minSnapshotIdx;
            try {
                IOUtils.deleteFile(new File(deleteFilePath));
                logger.info("Delete expired snapshot: {}", deleteFilePath);
            } catch (IOException e) {
                logger.error("Unable to remove expired snapshot: {}", deleteFilePath, e);
            }
        }
    }

    public CompletableFuture<Boolean> loadSnapshot() {
        // Check if still loading snapshot
        if (loadingSnapshot) {
            return CompletableFuture.completedFuture(false);
        }
        // Create snapshot reader
        SnapshotReader reader = snapshotStore.createSnapshotReader();
        if (reader == null) {
            return CompletableFuture.completedFuture(false);
        }
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        Closure closure = new Closure() {
            @Override
            public void done(Status status) {
                if (status.isOk()) {
                    future.complete(true);
                } else {
                    logger.error("Failed to load snapshot", status);
                    future.complete(false);
                }
            }
        };
        // Start loading snapshot
        this.loadingSnapshot = true;
        LoadSnapshotAfterHook loadSnapshotAfter = new LoadSnapshotAfterHook(reader, closure);
        if (!this.dLedgerServer.getFsmCaller().onSnapshotLoad(loadSnapshotAfter)) {
            this.dLedgerServer.getFsmCaller().setError(this.dLedgerServer,
                new DLedgerException(DLedgerResponseCode.LOAD_SNAPSHOT_ERROR, "Unable to call statemachine onSnapshotLoad"));
        }
        return future;
    }

    private void loadSnapshotAfter(SnapshotReader reader, SnapshotMeta snapshotMeta, SnapshotStatus status, Closure closure) {
        if (status.getCode() == SnapshotStatus.SUCCESS.getCode()) {
            this.lastSnapshotIndex = snapshotMeta.getLastIncludedIndex();
            this.lastSnapshotTerm = snapshotMeta.getLastIncludedTerm();
            this.loadingSnapshot = false;
            this.dLedgerServer.getDLedgerStore().reset(this.lastSnapshotIndex, this.lastSnapshotTerm);
            closure.done(Status.ok());
            logger.info("Snapshot {} loaded successfully", snapshotMeta);
        } else {
            closure.done(Status.error(DLedgerResponseCode.LOAD_SNAPSHOT_ERROR));
            // Stop the loading process if the snapshot is expired
            if (status.getCode() == SnapshotStatus.EXPIRED.getCode()) {
                this.loadingSnapshot = false;
                return;
            }
            // Remove the error snapshot
            boolean failed = false;
            try {
                IOUtils.deleteFile(new File(reader.getSnapshotStorePath()));
            } catch (IOException e) {
                logger.error("Unable to remove error snapshot: {}", reader.getSnapshotStorePath(), e);
                failed = true;
            }
            // Check if there is snapshot exists
            DLedgerConfig config = this.dLedgerServer.getDLedgerConfig();
            if (Objects.requireNonNull(new File(config.getSnapshotStoreBaseDir()).listFiles()).length == 0) {
                logger.error("No snapshot for recovering state machine: {}", config.getSnapshotStoreBaseDir());
                failed = true;
            }
            if (failed) {
                // Still able to recover from files if the beginning index of file store is 0
                if (this.dLedgerServer.getFsmCaller().getdLedgerStore().getLedgerBeforeBeginIndex() == -1) {
                    this.loadingSnapshot = false;
                    return;
                }
                this.dLedgerServer.getFsmCaller().setError(this.dLedgerServer,
                    new DLedgerException(DLedgerResponseCode.LOAD_SNAPSHOT_ERROR, "Fail to recover state machine"));
                return;
            }
            // Retry loading the previous snapshots
            logger.warn("Load snapshot from {} failed. Start recovering from the previous snapshot", reader.getSnapshotStorePath());
            this.loadingSnapshot = false;
            loadSnapshot();
        }
    }

    public SnapshotReader getSnapshotReaderIncludedTargetIndex(long index) {
        SnapshotReader reader = this.snapshotStore.createSnapshotReader();
        try {
            reader.load();
        } catch (Exception e) {
            logger.error("Load snapshot reader: {} meta failed", reader.getSnapshotStorePath(), e);
            return null;
        }
        if (reader.getSnapshotMeta().getLastIncludedIndex() < index) {
            return null;
        }
        return reader;
    }

    public boolean installSnapshot(DownloadSnapshot snapshot) {
        SnapshotMeta meta = snapshot.getMeta();
        if (meta.getLastIncludedTerm() < lastSnapshotTerm || (meta.getLastIncludedTerm() == lastSnapshotTerm && meta.getLastIncludedIndex() <= lastSnapshotIndex)) {
            logger.warn("Ignore installing snapshot {}, because the last applied snapshot is [term={}, index={}]", meta, lastSnapshotTerm, lastSnapshotIndex);
            return false;
        }
        SnapshotReader lastSnpReader = snapshotStore.createSnapshotReader();
        SnapshotMeta lastSnpMeta = null;
        if (lastSnpReader != null) {
            try {
                lastSnpMeta = lastSnpReader.load();
            } catch (Exception e) {
                logger.error("Load snapshot reader: {} meta failed", lastSnpReader.getSnapshotStorePath(), e);
                return false;
            }
        }
        if (lastSnpMeta != null &&
            (meta.getLastIncludedTerm() < lastSnpMeta.getLastIncludedTerm() ||
                (meta.getLastIncludedTerm() == lastSnpMeta.getLastIncludedTerm() && meta.getLastIncludedIndex() <= lastSnpMeta.getLastIncludedIndex()))) {
            logger.warn("Ignore installing snapshot {}, because the last saved snapshot is [term={}, index={}]", meta, lastSnpMeta.getLastIncludedTerm(), lastSnpMeta.getLastIncludedIndex());
            return false;
        }
        if (!snapshotStore.downloadSnapshot(snapshot)) {
            logger.warn("Install snapshot {} failed", meta);
            return false;
        }
        try {
            return this.loadSnapshot().get();
        } catch (Exception e) {
            logger.error("Install Snapshot and wait loading failed", e);
            return false;
        }
    }

    public long getLastSnapshotIndex() {
        return lastSnapshotIndex;
    }
}
