/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.entry.DLedgerEntryCoder;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFile;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
import io.openmessaging.storage.dledger.store.file.SelectMmapBufferResult;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFileCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(ReadFileCommand.class);

    @Parameter(names = {"--dir", "-d"}, description = "the data dir")
    private String dataDir = null;

    @Parameter(names = {"--pos", "-p"}, description = "the start pos")
    private long pos = 0;

    @Parameter(names = {"--size", "-s"}, description = "the file size")
    private int size = -1;

    @Parameter(names = {"--index", "-i"}, description = "the index")
    private long index = -1L;

    @Parameter(names = {"--body", "-b"}, description = "if read the body")
    private boolean readBody = false;

    @Override
    public void doCommand() {
        if (index != -1) {
            pos = index * DLedgerMmapFileStore.INDEX_UNIT_SIZE;
            if (size == -1) {
                size = DLedgerMmapFileStore.INDEX_UNIT_SIZE * 1024 * 1024;
            }
        } else {
            if (size == -1) {
                size = 1024 * 1024 * 1024;
            }
        }
        MmapFileList mmapFileList = new MmapFileList(dataDir, size);
        mmapFileList.load();
        MmapFile mmapFile = mmapFileList.findMappedFileByOffset(pos);
        if (mmapFile == null) {
            logger.info("Cannot find the file");
            return;
        }
        SelectMmapBufferResult result = mmapFile.selectMappedBuffer((int) (pos % size));
        ByteBuffer buffer = result.getByteBuffer();
        if (index != -1) {
            logger.info("magic={} pos={} size={} index={} term={}", buffer.getInt(), buffer.getLong(), buffer.getInt(), buffer.getLong(), buffer.getLong());
        } else {
            DLedgerEntry entry = DLedgerEntryCoder.decode(buffer, readBody);
            logger.info(JSON.toJSONString(entry));
        }
    }
}
