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

package io.openmessaging.storage.dledger.store;

import io.openmessaging.storage.dledger.ServerTestBase;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.openmessaging.storage.dledger.store.file.DefaultMmapFile.OS_PAGE_SIZE;
import static io.openmessaging.storage.dledger.store.file.MmapFileList.MIN_BLANK_LEN;

public class MmapFileListTest extends ServerTestBase {

    private void append(MmapFileList mmapFileList, int size, int entrySize) {
        for (int i = 0; i < size / entrySize; i++) {
            long prePos = mmapFileList.preAppend(entrySize);
            long appendPos = mmapFileList.append(new byte[entrySize]);
            Assertions.assertEquals(prePos, appendPos);
        }
    }

    @Test
    public void testTruncateAndReset() {
        String base = FileTestUtil.createTestDir();
        bases.add(base);
        {
            MmapFileList mmapFileList = new MmapFileList(base, 512);
            Assertions.assertEquals(0, mmapFileList.getMinOffset());
            Assertions.assertEquals(0, mmapFileList.getMaxWrotePosition());
            Assertions.assertEquals(0, mmapFileList.getMaxReadPosition());
            append(mmapFileList, (512 - MIN_BLANK_LEN) * 10, (512 - MIN_BLANK_LEN));
            Assertions.assertEquals(10, mmapFileList.getMappedFiles().size());
            Assertions.assertTrue(mmapFileList.checkSelf());
            Assertions.assertEquals(0, mmapFileList.getMinOffset());
            Assertions.assertEquals(512 * 10 - MIN_BLANK_LEN, mmapFileList.getMaxReadPosition());
            Assertions.assertEquals(512 * 10 - MIN_BLANK_LEN, mmapFileList.getMaxWrotePosition());
            while (true) {
                if (mmapFileList.commit(0) && mmapFileList.flush(0)) {
                    break;
                }
            }
            mmapFileList.truncateOffset(1536);
            Assertions.assertTrue(mmapFileList.checkSelf());
            Assertions.assertEquals(4, mmapFileList.getMappedFiles().size());
            Assertions.assertEquals(0, mmapFileList.getMinOffset());
            Assertions.assertEquals(1536, mmapFileList.getMaxReadPosition());
            Assertions.assertEquals(1536, mmapFileList.getMaxWrotePosition());

            mmapFileList.resetOffset(1024);
            Assertions.assertTrue(mmapFileList.checkSelf());
            Assertions.assertEquals(2, mmapFileList.getMappedFiles().size());
            Assertions.assertEquals(1024, mmapFileList.getMinOffset());
            Assertions.assertEquals(1536, mmapFileList.getMaxReadPosition());
            Assertions.assertEquals(1536, mmapFileList.getMaxWrotePosition());
        }

        {
            MmapFileList otherList = new MmapFileList(base, 512);
            otherList.load();
            Assertions.assertTrue(otherList.checkSelf());
            Assertions.assertEquals(2, otherList.getMappedFiles().size());
            Assertions.assertEquals(1024, otherList.getMinOffset());
            Assertions.assertEquals(2048, otherList.getMaxReadPosition());
            Assertions.assertEquals(2048, otherList.getMaxWrotePosition());

            otherList.truncateOffset(-1);
            Assertions.assertTrue(otherList.checkSelf());
            Assertions.assertEquals(0, otherList.getMappedFiles().size());
            Assertions.assertEquals(0, otherList.getMinOffset());
            Assertions.assertEquals(0, otherList.getMaxReadPosition());
            Assertions.assertEquals(0, otherList.getMaxWrotePosition());
        }

    }

    @Test
    public void testCommitAndFlush() {
        String base = FileTestUtil.createTestDir();
        bases.add(base);
        MmapFileList mmapFileList = new MmapFileList(base, OS_PAGE_SIZE + 2 + MIN_BLANK_LEN);
        mmapFileList.append(new byte[OS_PAGE_SIZE - 1]);
        mmapFileList.commit(1);
        Assertions.assertEquals(OS_PAGE_SIZE - 1, mmapFileList.getCommittedWhere());
        mmapFileList.flush(1);
        Assertions.assertEquals(0, mmapFileList.getFlushedWhere());

        mmapFileList.append(new byte[1]);
        mmapFileList.commit(1);
        Assertions.assertEquals(OS_PAGE_SIZE, mmapFileList.getCommittedWhere());
        mmapFileList.flush(1);
        Assertions.assertEquals(OS_PAGE_SIZE, mmapFileList.getFlushedWhere());

        mmapFileList.append(new byte[1]);
        mmapFileList.commit(0);
        Assertions.assertEquals(OS_PAGE_SIZE + 1, mmapFileList.getCommittedWhere());
        mmapFileList.flush(0);
        Assertions.assertEquals(OS_PAGE_SIZE + 1, mmapFileList.getFlushedWhere());

        mmapFileList.append(new byte[2]);
        mmapFileList.commit(1);
        Assertions.assertEquals(OS_PAGE_SIZE + 2 + MIN_BLANK_LEN, mmapFileList.getCommittedWhere());
        mmapFileList.flush(1);
        Assertions.assertEquals(OS_PAGE_SIZE + 2 + MIN_BLANK_LEN, mmapFileList.getFlushedWhere());

    }
}
