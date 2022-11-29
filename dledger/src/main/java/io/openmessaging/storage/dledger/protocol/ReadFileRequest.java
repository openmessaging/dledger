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

package io.openmessaging.storage.dledger.protocol;

public class ReadFileRequest extends RequestOrResponse {

    private String dataFileDir;

    private long pos;

    private int fileSize;

    private long index;

    private boolean readBodyOnly;

    public String getDataFileDir() {
        return dataFileDir;
    }

    public void setDataFileDir(String dataFileDir) {
        this.dataFileDir = dataFileDir;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public boolean isReadBodyOnly() {
        return readBodyOnly;
    }

    public void setReadBodyOnly(boolean readBodyOnly) {
        this.readBodyOnly = readBodyOnly;
    }
}
