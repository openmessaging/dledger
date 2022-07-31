package io.openmessaging.storage.dledger.snapshot;

import io.openmessaging.storage.dledger.statemachine.CommittedEntryIterator;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;

import com.alibaba.fastjson.JSON;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SnapshotWriterImpl implements SnapshotWriter {
    private String path = "./";
    // meta data
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    // used to get logs
    CommittedEntryIterator iter;

    public void init(String path){
        this.path = path;
    }

    public SnapshotWriterImpl(long lastIncludedIndex, long lastIncludedTerm, CommittedEntryIterator iter){
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedIndex = lastIncludedIndex;
        this.iter = iter;
    }

    @Override
    public void writeMeta(){
        // write meta data into meta file
        final File file = new File(path+"snapshot.meta");
        try (FileOutputStream fout = new FileOutputStream(file);
                BufferedOutputStream out = new BufferedOutputStream(fout)) {
                    final ByteBuffer buf = ByteBuffer.allocate(8);
                    buf.putLong(0, lastIncludedIndex);
                    out.write(buf.array());

                    final ByteBuffer buf2 = ByteBuffer.allocate(8);
                    buf.putLong(0, lastIncludedTerm);
                    out.write(buf2.array());
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void writeData(){
        // write data into data file
        final File file = new File(path+"snapshot.data");
        try (FileOutputStream fout = new FileOutputStream(file);
        BufferedOutputStream out = new BufferedOutputStream(fout)) {
            while (iter.hasNext()) {
                final DLedgerEntry next = iter.next();
                if (next != null) {
                    if (next.getIndex() <= lastIncludedIndex) {
                        out.write(JSON.toJSONBytes(next));
                    }
                }
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
        
    }
    @Override
    public String getPath(){
        return "./";
    }
}
