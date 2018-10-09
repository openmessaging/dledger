package org.apache.rocketmq.dleger.entry;

import java.nio.ByteBuffer;

public class DLegerEntryCoder {


    public static void encode(DLegerEntry entry, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        int size = entry.computSizeInBytes();
        byteBuffer.putInt(size);
        byteBuffer.putInt(entry.getMagic());
        byteBuffer.putLong(entry.getIndex());
        byteBuffer.putLong(entry.getTerm());
        byteBuffer.putInt(entry.getChainCrc());
        byteBuffer.putInt(entry.getBodyCrc());
        byteBuffer.putInt(entry.getBody().length);
        byteBuffer.put(entry.getBody());
        byteBuffer.flip();
    }

    public static void encodeIndex(long pos, int size, int magic, long index, long term, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        byteBuffer.putLong(pos);
        byteBuffer.putInt(size);
        byteBuffer.putInt(magic);
        byteBuffer.putLong(index);
        byteBuffer.putLong(term);
        byteBuffer.flip();
    }


    public static DLegerEntry decode(ByteBuffer byteBuffer) {
        DLegerEntry entry = new DLegerEntry();
        entry.setSize(byteBuffer.getInt());
        entry.setMagic(byteBuffer.getInt());
        entry.setIndex(byteBuffer.getLong());
        entry.setTerm(byteBuffer.getLong());
        entry.setChainCrc(byteBuffer.getInt());
        entry.setBodyCrc(byteBuffer.getInt());
        int bodySize = byteBuffer.getInt();
        byte[] body =  new byte[bodySize];
        byteBuffer.get(body);
        entry.setBody(body);
        return entry;
    }

    public static void setIndexTerm(ByteBuffer byteBuffer, long index, long term, int magic) {
        byteBuffer.mark();
        byteBuffer.position(byteBuffer.position() + 8);
        byteBuffer.putLong(index);
        byteBuffer.putLong(term);
        byteBuffer.reset();
    }


}
