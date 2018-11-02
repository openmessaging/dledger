package org.apache.rocketmq.dleger.entry;

import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

public class DLegerEntryCoderTest {

    @Test
    public void testEncodeDecode() {
        DLegerEntry entry = new DLegerEntry();
        entry.setBody(new byte[100]);
        entry.setBodyCrc(111);
        entry.setChainCrc(222);
        entry.setTerm(333);
        entry.setIndex(444);
        entry.setMagic(666);
        entry.setChannel(12);
        entry.computSizeInBytes();

        ByteBuffer buffer = ByteBuffer.allocate(entry.getSize());
        DLegerEntryCoder.encode(entry, buffer);
        Assert.assertEquals(entry.getSize(), buffer.remaining());

        long pos = DLegerEntryCoder.getPos(buffer);
        Assert.assertEquals(pos, entry.getPos());

        DLegerEntry another = DLegerEntryCoder.decode(buffer);

        Assert.assertEquals(another.getSize(), entry.getSize());
        Assert.assertEquals(another.getMagic(), entry.getMagic());
        Assert.assertEquals(another.getIndex(), entry.getIndex());
        Assert.assertEquals(another.getTerm(), entry.getTerm());
        Assert.assertEquals(another.getPos(), entry.getPos());
        Assert.assertEquals(another.getChannel(), entry.getChannel());
        Assert.assertEquals(another.getChainCrc(), entry.getChainCrc());
        Assert.assertEquals(another.getBodyCrc(), entry.getBodyCrc());
        Assert.assertArrayEquals(another.getBody(), entry.getBody());

    }

    @Test
    public void testCompareEntry() {
        DLegerEntry entry = new DLegerEntry();
        DLegerEntry other = new DLegerEntry();
        Assert.assertTrue(!entry.equals(null));
        Assert.assertEquals(entry, other);
        Assert.assertEquals(other, entry);
        entry.setBody(new byte[0]);
        Assert.assertNotEquals(entry, other);
        Assert.assertNotEquals(other, entry);
        other.setBody(new byte[0]);
        Assert.assertEquals(entry, other);
        entry.setBodyCrc(123);
        other.setBodyCrc(456);
        Assert.assertEquals(entry, other);
        entry.setChainCrc(123);
        other.setChainCrc(456);
        Assert.assertEquals(entry, other);
        entry.setChannel(1);
        Assert.assertNotEquals(entry, other);
        other.setChannel(1);
        Assert.assertEquals(entry, other);
        entry.setPos(123);
        Assert.assertNotEquals(entry, other);
        other.setPos(123);
        Assert.assertEquals(entry, other);
    }
}
