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

package io.openmessaging.storage.dledger.entry;

import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

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
        entry.computSizeInBytes();

        ByteBuffer buffer = ByteBuffer.allocate(entry.getSize());
        DLedgerEntryCoder.encode(entry, buffer);
        Assert.assertEquals(entry.getSize(), buffer.remaining());

        long pos = DLedgerEntryCoder.getPos(buffer);
        Assert.assertEquals(pos, entry.getPos());

        buffer.mark();
        DLedgerEntry another = DLedgerEntryCoder.decode(buffer);
        buffer.reset();

        Assert.assertEquals(another.getSize(), entry.getSize());
        Assert.assertEquals(another.getMagic(), entry.getMagic());
        Assert.assertEquals(another.getIndex(), entry.getIndex());
        Assert.assertEquals(another.getTerm(), entry.getTerm());
        Assert.assertEquals(another.getPos(), entry.getPos());
        Assert.assertEquals(another.getChannel(), entry.getChannel());
        Assert.assertEquals(another.getChainCrc(), entry.getChainCrc());
        Assert.assertEquals(another.getBodyCrc(), entry.getBodyCrc());
        Assert.assertArrayEquals(another.getBody(), entry.getBody());

        buffer.mark();
        buffer.position(DLedgerEntry.BODY_OFFSET - 4);
        buffer.putInt(Integer.MAX_VALUE);
        buffer.reset();

        DLedgerEntry nullBodyEntry = DLedgerEntryCoder.decode(buffer);
        Assert.assertNull(nullBodyEntry.getBody());

    }

    @Test
    public void testCompareEntry() {
        DLedgerEntry entry = new DLedgerEntry();
        DLedgerEntry other = new DLedgerEntry();
        Assert.assertTrue(!entry.equals(null));
        Assert.assertEquals(entry, other);
        Assert.assertEquals(other, entry);
        Assert.assertEquals(other.hashCode(), entry.hashCode());
        entry.setBody(new byte[0]);
        Assert.assertNotEquals(entry, other);
        Assert.assertNotEquals(other, entry);
        Assert.assertNotEquals(entry.hashCode(), other.hashCode());
        other.setBody(new byte[0]);
        Assert.assertEquals(entry, other);
        Assert.assertEquals(entry.hashCode(), other.hashCode());
        entry.setBodyCrc(123);
        other.setBodyCrc(456);
        Assert.assertEquals(entry, other);
        Assert.assertEquals(entry.hashCode(), other.hashCode());
        entry.setChainCrc(123);
        other.setChainCrc(456);
        Assert.assertEquals(entry, other);
        Assert.assertEquals(entry.hashCode(), other.hashCode());
        entry.setChannel(1);
        Assert.assertNotEquals(entry, other);
        Assert.assertNotEquals(entry.hashCode(), other.hashCode());
        other.setChannel(1);
        Assert.assertEquals(entry, other);
        Assert.assertEquals(entry.hashCode(), other.hashCode());
        entry.setPos(123);
        Assert.assertNotEquals(entry, other);
        Assert.assertNotEquals(entry.hashCode(), other.hashCode());
        other.setPos(123);
        Assert.assertEquals(entry, other);
        Assert.assertEquals(entry.hashCode(), other.hashCode());
    }
}
