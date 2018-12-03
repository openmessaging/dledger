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

package io.openmessaging.storage.dledger.store;

import io.openmessaging.storage.dledger.ServerTestBase;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import org.junit.Assert;
import org.junit.Test;

import static io.openmessaging.storage.dledger.store.file.DefaultMmapFile.OS_PAGE_SIZE;
import static io.openmessaging.storage.dledger.store.file.MmapFileList.MIN_BLANK_LEN;

public class MmapFileListTest extends ServerTestBase {

    private void append(MmapFileList mmapFileList, int size, int entrySize) {
        for (int i = 0; i < size / entrySize; i++) {
            long prePos = mmapFileList.preAppend(entrySize);
            long appendPos = mmapFileList.append(new byte[entrySize]);
            Assert.assertEquals(prePos, appendPos);
        }
    }

    @Test
    public void testTruncateAndReset() {
        String base = FileTestUtil.createTestDir();
        bases.add(base);
        {
            MmapFileList mmapFileList = new MmapFileList(base, 512);
            Assert.assertEquals(0, mmapFileList.getMinOffset());
            Assert.assertEquals(0, mmapFileList.getMaxWrotePosition());
            Assert.assertEquals(0, mmapFileList.getMaxReadPosition());
            append(mmapFileList, (512 - MIN_BLANK_LEN) * 10, (512 - MIN_BLANK_LEN));
            Assert.assertEquals(10, mmapFileList.getMappedFiles().size());
            Assert.assertTrue(mmapFileList.checkSelf());
            Assert.assertEquals(0, mmapFileList.getMinOffset());
            Assert.assertEquals(512 * 10 - MIN_BLANK_LEN, mmapFileList.getMaxReadPosition());
            Assert.assertEquals(512 * 10 - MIN_BLANK_LEN, mmapFileList.getMaxWrotePosition());
            while (true) {
                if (mmapFileList.commit(0) && mmapFileList.flush(0)) {
                    break;
                }
            }
            mmapFileList.truncateOffset(1536);
            Assert.assertTrue(mmapFileList.checkSelf());
            Assert.assertEquals(4, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, mmapFileList.getMinOffset());
            Assert.assertEquals(1536, mmapFileList.getMaxReadPosition());
            Assert.assertEquals(1536, mmapFileList.getMaxWrotePosition());

            mmapFileList.resetOffset(1024);
            Assert.assertTrue(mmapFileList.checkSelf());
            Assert.assertEquals(2, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(1024, mmapFileList.getMinOffset());
            Assert.assertEquals(1536, mmapFileList.getMaxReadPosition());
            Assert.assertEquals(1536, mmapFileList.getMaxWrotePosition());
        }

        {
            MmapFileList otherList = new MmapFileList(base, 512);
            otherList.load();
            Assert.assertTrue(otherList.checkSelf());
            Assert.assertEquals(2, otherList.getMappedFiles().size());
            Assert.assertEquals(1024, otherList.getMinOffset());
            Assert.assertEquals(2048, otherList.getMaxReadPosition());
            Assert.assertEquals(2048, otherList.getMaxWrotePosition());

            otherList.truncateOffset(-1);
            Assert.assertTrue(otherList.checkSelf());
            Assert.assertEquals(0, otherList.getMappedFiles().size());
            Assert.assertEquals(0, otherList.getMinOffset());
            Assert.assertEquals(0, otherList.getMaxReadPosition());
            Assert.assertEquals(0, otherList.getMaxWrotePosition());
        }

    }

    @Test
    public void testCommitAndFlush() {
        String base = FileTestUtil.createTestDir();
        bases.add(base);
        MmapFileList mmapFileList = new MmapFileList(base, OS_PAGE_SIZE + 2 + MIN_BLANK_LEN);
        mmapFileList.append(new byte[OS_PAGE_SIZE - 1]);
        mmapFileList.commit(1);
        Assert.assertEquals(OS_PAGE_SIZE - 1, mmapFileList.getCommittedWhere());
        mmapFileList.flush(1);
        Assert.assertEquals(0, mmapFileList.getFlushedWhere());

        mmapFileList.append(new byte[1]);
        mmapFileList.commit(1);
        Assert.assertEquals(OS_PAGE_SIZE, mmapFileList.getCommittedWhere());
        mmapFileList.flush(1);
        Assert.assertEquals(OS_PAGE_SIZE, mmapFileList.getFlushedWhere());

        mmapFileList.append(new byte[1]);
        mmapFileList.commit(0);
        Assert.assertEquals(OS_PAGE_SIZE + 1, mmapFileList.getCommittedWhere());
        mmapFileList.flush(0);
        Assert.assertEquals(OS_PAGE_SIZE + 1, mmapFileList.getFlushedWhere());

        mmapFileList.append(new byte[2]);
        mmapFileList.commit(1);
        Assert.assertEquals(OS_PAGE_SIZE + 2 + MIN_BLANK_LEN, mmapFileList.getCommittedWhere());
        mmapFileList.flush(1);
        Assert.assertEquals(OS_PAGE_SIZE + 2 + MIN_BLANK_LEN, mmapFileList.getFlushedWhere());

    }
}
