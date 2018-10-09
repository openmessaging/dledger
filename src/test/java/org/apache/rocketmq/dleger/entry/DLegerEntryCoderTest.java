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
        entry.computSizeInBytes();

        ByteBuffer buffer = ByteBuffer.allocate(entry.getSize());
        DLegerEntryCoder.encode(entry, buffer);
        Assert.assertEquals(entry.getSize(), buffer.remaining());

        DLegerEntry another = DLegerEntryCoder.decode(buffer);

        Assert.assertEquals(another.getSize(), entry.getSize());
        Assert.assertEquals(another.getMagic(), entry.getMagic());
        Assert.assertEquals(another.getIndex(), entry.getIndex());
        Assert.assertEquals(another.getTerm(), entry.getTerm());
        Assert.assertEquals(another.getChainCrc(), entry.getChainCrc());
        Assert.assertEquals(another.getBodyCrc(), entry.getBodyCrc());
        Assert.assertArrayEquals(another.getBody(), entry.getBody());

    }
}
