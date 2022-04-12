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

package io.openmessaging.storage.dledger.entry;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DLedgerEntryCoderTest {

    @Test
    public void testEncodeDecode() {
        DLedgerEntry entry = new DLedgerEntry();
        entry.setBody(new byte[100]);
        entry.setBodyCrc(111);
        entry.setChainCrc(222);
        entry.setTerm(333);
        entry.setIndex(444);
        entry.setMagic(666);
        entry.setChannel(12);
        entry.computeSizeInBytes();

        ByteBuffer buffer = ByteBuffer.allocate(entry.getSize());
        DLedgerEntryCoder.encode(entry, buffer);
        Assertions.assertEquals(entry.getSize(), buffer.remaining());

        long pos = DLedgerEntryCoder.getPos(buffer);
        Assertions.assertEquals(pos, entry.getPos());

        buffer.mark();
        DLedgerEntry another = DLedgerEntryCoder.decode(buffer);
        buffer.reset();

        Assertions.assertEquals(another.getSize(), entry.getSize());
        Assertions.assertEquals(another.getMagic(), entry.getMagic());
        Assertions.assertEquals(another.getIndex(), entry.getIndex());
        Assertions.assertEquals(another.getTerm(), entry.getTerm());
        Assertions.assertEquals(another.getPos(), entry.getPos());
        Assertions.assertEquals(another.getChannel(), entry.getChannel());
        Assertions.assertEquals(another.getChainCrc(), entry.getChainCrc());
        Assertions.assertEquals(another.getBodyCrc(), entry.getBodyCrc());
        Assertions.assertArrayEquals(another.getBody(), entry.getBody());

        buffer.mark();
        buffer.position(DLedgerEntry.BODY_OFFSET - 4);
        buffer.putInt(Integer.MAX_VALUE);
        buffer.reset();

        DLedgerEntry nullBodyEntry = DLedgerEntryCoder.decode(buffer);
        Assertions.assertNull(nullBodyEntry.getBody());

    }

    @Test
    public void testCompareEntry() {
        DLedgerEntry entry = new DLedgerEntry();
        DLedgerEntry other = new DLedgerEntry();
        Assertions.assertTrue(!entry.equals(null));
        Assertions.assertEquals(entry, other);
        Assertions.assertEquals(other, entry);
        Assertions.assertEquals(other.hashCode(), entry.hashCode());
        entry.setBody(new byte[0]);
        Assertions.assertNotEquals(entry, other);
        Assertions.assertNotEquals(other, entry);
        Assertions.assertNotEquals(entry.hashCode(), other.hashCode());
        other.setBody(new byte[0]);
        Assertions.assertEquals(entry, other);
        Assertions.assertEquals(entry.hashCode(), other.hashCode());
        entry.setBodyCrc(123);
        other.setBodyCrc(456);
        Assertions.assertEquals(entry, other);
        Assertions.assertEquals(entry.hashCode(), other.hashCode());
        entry.setChainCrc(123);
        other.setChainCrc(456);
        Assertions.assertEquals(entry, other);
        Assertions.assertEquals(entry.hashCode(), other.hashCode());
        entry.setChannel(1);
        Assertions.assertNotEquals(entry, other);
        Assertions.assertNotEquals(entry.hashCode(), other.hashCode());
        other.setChannel(1);
        Assertions.assertEquals(entry, other);
        Assertions.assertEquals(entry.hashCode(), other.hashCode());
        entry.setPos(123);
        Assertions.assertNotEquals(entry, other);
        Assertions.assertNotEquals(entry.hashCode(), other.hashCode());
        other.setPos(123);
        Assertions.assertEquals(entry, other);
        Assertions.assertEquals(entry.hashCode(), other.hashCode());
    }
}
