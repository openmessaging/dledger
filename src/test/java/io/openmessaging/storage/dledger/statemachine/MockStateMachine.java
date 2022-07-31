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

import java.util.concurrent.CompletableFuture;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;

public class MockStateMachine implements StateMachine {

    private volatile long appliedIndex = -1;
    private volatile long totalEntries;
    private volatile int           saveSnapshotTimes;
    private volatile int           loadSnapshotTimes;

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
            }
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final CompletableFuture<Boolean> done) {
        // this.saveSnapshotTimes++;
        // final String path = writer.getPath() + File.separator + "data";
        // final File file = new File(path);
        // try (FileOutputStream fout = new FileOutputStream(file);
        //         BufferedOutputStream out = new BufferedOutputStream(fout)) {
        //     this.lock.lock();
        //     try {
        //         for (final ByteBuffer buf : this.logs) {
        //             final byte[] bs = new byte[4];
        //             Bits.putInt(bs, 0, buf.remaining());
        //             out.write(bs);
        //             out.write(buf.array());
        //         }
        //         this.snapshotIndex = this.appliedIndex;
        //     } finally {
        //         this.lock.unlock();
        //     }
        //     System.out.println("Node<" + this.address + "> saved snapshot into " + file);
        //     writer.addFile("data");
        //     done.run(Status.OK());
        // } catch (final IOException e) {
        //     e.printStackTrace();
        //     done.run(new Status(RaftError.EIO, "Fail to save snapshot"));
        // }
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        // SnapshotMeta meta = reader.load();
        // this.lastAppliedIndex.set(meta.getLastIncludedIndex());
        // this.loadSnapshotTimes++;
        // final String path = reader.getPath() + File.separator + "data";
        // final File file = new File(path);
        // if (!file.exists()) {
        //     return false;
        // }
        // try (FileInputStream fin = new FileInputStream(file); BufferedInputStream in = new BufferedInputStream(fin)) {
        //     this.lock.lock();
        //     this.logs.clear();
        //     try {
        //         while (true) {
        //             final byte[] bs = new byte[4];
        //             if (in.read(bs) == 4) {
        //                 final int len = Bits.getInt(bs, 0);
        //                 final byte[] buf = new byte[len];
        //                 if (in.read(buf) != len) {
        //                     break;
        //                 }
        //                 this.logs.add(ByteBuffer.wrap(buf));
        //             } else {
        //                 break;
        //             }
        //         }
        //     } finally {
        //         this.lock.unlock();
        //     }
        //     System.out.println("Node<" + this.address + "> loaded snapshot from " + path);
        //     return true;
        // } catch (final IOException e) {
        //     e.printStackTrace();
        //     return false;
        // }
        return false;
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
}
