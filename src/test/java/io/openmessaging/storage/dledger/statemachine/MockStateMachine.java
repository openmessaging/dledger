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

package io.openmessaging.storage.dledger.statemachine;

import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import com.alibaba.fastjson.JSON;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;

public class MockStateMachine implements StateMachine {

    private volatile long appliedIndex = -1;
    private volatile long totalEntries;
    private final List<ByteBuffer> logs = new ArrayList<>(); // add logs to store the log entries
    @Override
    public void onApply(final CommittedEntryIterator iter) {
        while (iter.hasNext()) {
            final DLedgerEntry next = iter.next();
            if (next != null) {
                if (next.getIndex() <= this.appliedIndex) {
                    continue;
                }
                this.appliedIndex = next.getIndex();
                this.totalEntries += 1;
                this.logs.add(ByteBuffer.wrap(JSON.toJSONBytes(next))); // add the log entry to the logs using JSON
            }
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer) {
        // write data into data file
        final String path = writer.getPath();
        final File file = new File(path+"snapshot.data");
        try (FileOutputStream fout = new FileOutputStream(file);
        BufferedOutputStream out = new BufferedOutputStream(fout)) {
            for(final ByteBuffer buf : this.logs){
                final byte[] bs = new byte[4];
                bs[0] = (byte) (buf.remaining() >>> 24);
                bs[0 + 1] = (byte) (buf.remaining() >>> 16);
                bs[0 + 2] = (byte) (buf.remaining() >>> 8);
                bs[0 + 3] = (byte) buf.remaining();
                out.write(bs);
                out.write(buf.array());
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<DLedgerEntry> onSnapshotLoad(final SnapshotReader reader) {
        final String path = "./snapshot.data";
        final File file = new File(path);

        try (FileInputStream fin = new FileInputStream(file); BufferedInputStream in = new BufferedInputStream(fin)) {
            this.logs.clear();
            try {
                while (true) {
                    final byte[] bs = new byte[4];
                    if (in.read(bs) == 4) {
                        final int len = (bs[0] & 0xff) << 24 | (bs[0 + 1] & 0xff) << 16 | (bs[0 + 2] & 0xff) << 8 | bs[0 + 3] & 0xff;
                        final byte[] buf = new byte[len];
                        if (in.read(buf) != len) {
                            break;
                        }
                        this.logs.add(ByteBuffer.wrap(buf));
                    } else {
                        break;
                    }
                }
            } finally {
                ;
            }
            // System.out.println(this.logs.size());
            List<DLedgerEntry> Dlogs = new ArrayList<>();
            for(final ByteBuffer buf : this.logs){
                Dlogs.add(JSON.parseObject(buf.array(), DLedgerEntry.class));
            }
            return Dlogs;
        // try (FileInputStream fin = new FileInputStream(file); BufferedInputStream in = new BufferedInputStream(fin);
        //     InputStreamReader inputstreamreader = new InputStreamReader(fin, "UTF-8");
        //     BufferedReader bufferedreader = new BufferedReader(inputstreamreader)) {
        //     String laststr = "";
        //     try {
        //         String tempString = null;
        //         while((tempString = bufferedreader.readLine()) != null){
        //             laststr += tempString;
        //         }
        //         bufferedreader.close();
        //     } finally {
        //         ;
        //     }

            // // read data
            // List<DLedgerEntry> list = JSON.parseArray(laststr, DLedgerEntry.class);
            // for(DLedgerEntry i:list){
            //     dLedgerStore.appendAsLeader(i);
            // }
        } catch (final IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    @Override
    public void onShutdown() {

    }

    public long getAppliedIndex() {
        return this.appliedIndex;
    }

    public long getTotalEntries() {
        return totalEntries;
    }

    public List<ByteBuffer> getLogs() {
            return this.logs;
    }
}
