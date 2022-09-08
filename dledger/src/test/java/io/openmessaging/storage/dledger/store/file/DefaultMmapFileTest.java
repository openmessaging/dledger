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

package io.openmessaging.storage.dledger.store.file;

import io.openmessaging.storage.dledger.ServerTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DefaultMmapFileTest extends ServerTestBase {

    @Test
    public void testDefaultMmapFile() throws Exception{

        String path = "/tmp/file/a/";
        bases.add(path);
        DefaultMmapFile file = new DefaultMmapFile(path+"0000",1023);

        file.setStartPosition(1);
        file.setWrotePosition(2);
        file.setFlushedPosition(3);
        file.setCommittedPosition(1);

        Assertions.assertEquals(1, file.getStartPosition());
        Assertions.assertEquals(2, file.getWrotePosition());
        Assertions.assertEquals(3, file.getFlushedPosition());
        Assertions.assertEquals(2, file.commit(1));
    }


}