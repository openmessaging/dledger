package io.openmessaging.storage.dleger.entry;

import java.nio.ByteBuffer;

public class DLegerEntryCoder {


    public static void encode(DLegerEntry entry, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        int size = entry.computSizeInBytes();
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


    public static DLegerEntry decode(ByteBuffer byteBuffer) {
       return decode(byteBuffer, true);
    }

    public static DLegerEntry decode(ByteBuffer byteBuffer, boolean readBody) {
        DLegerEntry entry = new DLegerEntry();
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
            byte[] body =  new byte[bodySize];
            byteBuffer.get(body);
            entry.setBody(body);
        }
        return entry;
    }

    public static void setPos(ByteBuffer byteBuffer, long pos) {
        byteBuffer.mark();
        byteBuffer.position(byteBuffer.position() + DLegerEntry.POS_OFFSET);
        byteBuffer.putLong(pos);
        byteBuffer.reset();
    }

    public static long  getPos(ByteBuffer byteBuffer) {
        long pos;
        byteBuffer.mark();
        byteBuffer.position(byteBuffer.position() + DLegerEntry.POS_OFFSET);
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
