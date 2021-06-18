/*
 * Copyright 2017-2020 the original author or authors.
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

package io.openmessaging.storage.dledger.entry;

import java.nio.ByteBuffer;

public class DLedgerEntryCoder {

    public static void encode(DLedgerEntry entry, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        int size = entry.computeSizeInBytes();
        //always put magic on the first position
        byteBuffer.putInt(entry.getMagic());
        byteBuffer.putInt(size);
        byteBuffer.putLong(entry.getIndex());
        byteBuffer.putLong(entry.getTerm());
        byteBuffer.putLong(entry.getPos());
        byteBuffer.putInt(entry.getChannel());
        byteBuffer.putInt(entry.getChainCrc());
        byteBuffer.putInt(entry.getBodyCrc());
        byteBuffer.putInt(entry.getBody().length);
        byteBuffer.put(entry.getBody());
        byteBuffer.flip();
    }

    public static void encodeIndex(long pos, int size, int magic, long index, long term, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        byteBuffer.putInt(magic);
        byteBuffer.putLong(pos);
        byteBuffer.putInt(size);
        byteBuffer.putLong(index);
        byteBuffer.putLong(term);
        byteBuffer.flip();
    }

    public static DLedgerEntry decode(ByteBuffer byteBuffer) {
        return decode(byteBuffer, true);
    }

    public static DLedgerEntry decode(ByteBuffer byteBuffer, boolean readBody) {
        DLedgerEntry entry = new DLedgerEntry();
        entry.setMagic(byteBuffer.getInt());
        entry.setSize(byteBuffer.getInt());
        entry.setIndex(byteBuffer.getLong());
        entry.setTerm(byteBuffer.getLong());
        entry.setPos(byteBuffer.getLong());
        entry.setChannel(byteBuffer.getInt());
        entry.setChainCrc(byteBuffer.getInt());
        entry.setBodyCrc(byteBuffer.getInt());
        int bodySize = byteBuffer.getInt();
        if (readBody && bodySize < entry.getSize()) {
            byte[] body = new byte[bodySize];
            byteBuffer.get(body);
            entry.setBody(body);
        }
        return entry;
    }

    public static void setPos(ByteBuffer byteBuffer, long pos) {
        byteBuffer.mark();
        byteBuffer.position(byteBuffer.position() + DLedgerEntry.POS_OFFSET);
        byteBuffer.putLong(pos);
        byteBuffer.reset();
    }

    public static long getPos(ByteBuffer byteBuffer) {
        long pos;
        byteBuffer.mark();
        byteBuffer.position(byteBuffer.position() + DLedgerEntry.POS_OFFSET);
        pos = byteBuffer.getLong();
        byteBuffer.reset();
        return pos;
    }

    public static void setIndexTerm(ByteBuffer byteBuffer, long index, long term, int magic) {
        byteBuffer.mark();
        byteBuffer.putInt(magic);
        byteBuffer.position(byteBuffer.position() + 4);
        byteBuffer.putLong(index);
        byteBuffer.putLong(term);
        byteBuffer.reset();
    }

}
