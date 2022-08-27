package io.openmessaging.storage.dledger.snapshot;

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

    public void init(String path){
        this.path = path;
    }

    public SnapshotWriterImpl(long lastIncludedIndex, long lastIncludedTerm){
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedIndex = lastIncludedIndex;
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
        ;
    }
    @Override
    public String getPath(){
        return "./";
    }
}
