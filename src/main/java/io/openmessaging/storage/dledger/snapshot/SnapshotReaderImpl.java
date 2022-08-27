package io.openmessaging.storage.dledger.snapshot;

import com.alibaba.fastjson.JSON;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

public class SnapshotReaderImpl implements SnapshotReader {

    private String path;
    @Override
    public void setSnapshotFilePath(String path){
        this.path = path;
    }//设置snapshot⽂件保存路径

    @Override
    public void setSnapshot(){;}

    @Override
    public long getLastIncludedIndex(){
        File file = new File("./snapshot.meta");
        try (FileInputStream fin = new FileInputStream(file); BufferedInputStream in = new BufferedInputStream(fin);
            BufferedInputStream reader = new BufferedInputStream(fin)) {

            byte[] bytearray = new byte[8];
            reader.read(bytearray);
            ByteBuffer buffer = ByteBuffer.wrap(bytearray);
            return buffer.getLong();
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return 0;}
}
