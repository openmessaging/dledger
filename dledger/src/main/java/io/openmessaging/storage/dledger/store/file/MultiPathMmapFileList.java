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

package io.openmessaging.storage.dledger.store.file;

import io.netty.util.internal.StringUtil;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class MultiPathMmapFileList extends MmapFileList {

    private final Supplier<Set<String>> fullStorePathsSupplier;
    private final DLedgerConfig config;

    public MultiPathMmapFileList(DLedgerConfig config, int mappedFileSize, Supplier<Set<String>> fullStorePathsSupplier) {
        super(config.getDataStorePath(), mappedFileSize);
        this.config = config;
        this.fullStorePathsSupplier = fullStorePathsSupplier;
    }

    private Set<String> getPaths() {
        String[] paths = this.config.getDataStorePath().trim().split(DLedgerConfig.MULTI_PATH_SPLITTER);
        return new HashSet<>(Arrays.asList(paths));
    }

    private Set<String> getReadonlyPaths() {
        String pathStr = config.getReadOnlyDataStoreDirs();
        if (StringUtil.isNullOrEmpty(pathStr)) {
            return Collections.emptySet();
        }
        String[] paths = pathStr.trim().split(DLedgerConfig.MULTI_PATH_SPLITTER);
        return new HashSet<>(Arrays.asList(paths));
    }

    @Override
    public boolean load() {
        Set<String> storePathSet = getPaths();
        storePathSet.addAll(getReadonlyPaths());

        List<File> files = new ArrayList<>();
        for (String path : storePathSet) {
            File dir = new File(path);
            File[] ls = dir.listFiles();
            if (ls != null) {
                Collections.addAll(files, ls);
            }
        }

        return doLoad(files);
    }

    @Override
    protected MmapFile tryCreateMappedFile(long createOffset) {
        long fileIdx = createOffset / this.getMappedFileSize();
        Set<String> storePath = getPaths();
        Set<String> readonlyPathSet = getReadonlyPaths();
        Set<String> fullStorePaths =
                fullStorePathsSupplier == null ? Collections.emptySet() : fullStorePathsSupplier.get();


        HashSet<String> availableStorePath = new HashSet<>(storePath);
        //do not create file in readonly store path.
        availableStorePath.removeAll(readonlyPathSet);

        //do not create file is space is nearly full.
        availableStorePath.removeAll(fullStorePaths);

        //if no store path left, fall back to writable store path.
        if (availableStorePath.isEmpty()) {
            availableStorePath = new HashSet<>(storePath);
            availableStorePath.removeAll(readonlyPathSet);
        }

        String[] paths = availableStorePath.toArray(new String[]{});
        Arrays.sort(paths);
        String nextFilePath = paths[(int) (fileIdx % paths.length)] + File.separator
                + DLedgerUtils.offset2FileName(createOffset);
        return doCreateMappedFile(nextFilePath);
    }

    @Override
    public void destroy() {
        for (MmapFile mf : this.getMappedFiles()) {
            mf.destroy(1000 * 3);
        }
        this.getMappedFiles().clear();
        this.setFlushedWhere(0);

        Set<String> storePathSet = getPaths();
        storePathSet.addAll(getReadonlyPaths());

        for (String path : storePathSet) {
            File file = new File(path);
            if (file.isDirectory()) {
                file.delete();
            }
        }
    }
}
