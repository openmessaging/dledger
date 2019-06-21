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

package io.openmessaging.storage.dledger.cmdline;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;

import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * A benchmark tool for DLedger
 * java -classpath DLedger.jar io.openmessaging.storage.dledger.cmdline.Benchmark
 */
public class Benchmark {

    static class BenchmarkParameter {

        @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server. Default n0-localhost:20911")
        private String peers = "n0-localhost:20911";

        @Parameter(names = {"--group", "-g"}, description = "Group of this server. Default default")
        private String group = "default";

        @Parameter(names = {"--threads", "-t"}, description = "Number of threads to perform requests. Default 1")
        private int threads = 1;

        @Parameter(names = {"--count", "-n"}, description = "Number of total requests to perform for benchmarking. Default 10000")
        private long counts = 10000;

        @Parameter(names = {"--size", "-s"}, description = "Size in bytes of every request entry. Default 100")
        private int entrySize = 100;


        public String getPeers() {
            return peers;
        }

        public void setPeers(String peers) {
            this.peers = peers;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }

        public long getCounts() {
            return counts;
        }

        public void setCounts(long counts) {
            this.counts = counts;
        }

        public int getEntrySize() {
            return entrySize;
        }

        public void setEntrySize(int entrySize) {
            this.entrySize = entrySize;
        }
    }

    static class BenchmarkStatics {
        private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);

        private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);

        private final AtomicLong receiveResponseSuccessCount = new AtomicLong(0L);

        private final AtomicLong receiveResponseFailedCount = new AtomicLong(0L);

        private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);

        private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

        public long[] createSnapshot() {
            long[] snap = new long[]{
                    System.currentTimeMillis(),
                    this.sendRequestSuccessCount.get(),
                    this.sendRequestFailedCount.get(),
                    this.receiveResponseSuccessCount.get(),
                    this.receiveResponseFailedCount.get(),
                    this.sendMessageSuccessTimeTotal.get(),
            };

            return snap;
        }

        public AtomicLong getSendRequestSuccessCount() {
            return sendRequestSuccessCount;
        }

        public AtomicLong getSendRequestFailedCount() {
            return sendRequestFailedCount;
        }

        public AtomicLong getReceiveResponseSuccessCount() {
            return receiveResponseSuccessCount;
        }

        public AtomicLong getReceiveResponseFailedCount() {
            return receiveResponseFailedCount;
        }

        public AtomicLong getSendMessageSuccessTimeTotal() {
            return sendMessageSuccessTimeTotal;
        }

        public AtomicLong getSendMessageMaxRT() {
            return sendMessageMaxRT;
        }
    }

    static class BenchmarkStaticsSnapshots {
        private final int capacity;

        private LinkedList<long[]> snapshotList;

        public BenchmarkStaticsSnapshots(int capacity) {
            this.capacity = capacity;
            this.snapshotList = new LinkedList<>();
        }

        public long[] addSnapshot(long[] snapshot) {
            this.snapshotList.addLast(snapshot);
            if (snapshotList.size() > capacity) {
                return snapshotList.removeFirst();
            } else {
                return null;
            }
        }

        public long[] head() {
            return snapshotList.getFirst();
        }

        public long[] tail() {
            return snapshotList.getLast();
        }

        public boolean full() {
            return this.snapshotList.size() >= capacity;
        }

    }


    static class PrintTask extends TimerTask {
        final BenchmarkStatics statics;
        final BenchmarkStaticsSnapshots samples;

        public PrintTask(BenchmarkStatics statics, BenchmarkStaticsSnapshots samples) {
            this.statics = statics;
            this.samples = samples;
        }

        @Override
        public void run() {
            if (samples.full()) {
                long[] begin = samples.head();
                long[] end = samples.tail();
                final long sendTps = (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
                final double averageRT = (end[5] - begin[5]) / (double) (end[3] - begin[3]);

                System.out.printf("Send TPS: %d Max RT: %d Average RT: %7.3f Send Failed: %d Response Failed: %d%n",
                        sendTps, this.statics.getSendMessageMaxRT().get(), averageRT, end[2], end[4]);
            }
        }
    }

    static class SnapshotTask extends TimerTask {
        final BenchmarkStatics statics;
        final BenchmarkStaticsSnapshots samples;

        public SnapshotTask(BenchmarkStatics statics, BenchmarkStaticsSnapshots samples) {
            this.statics = statics;
            this.samples = samples;

        }

        @Override
        public void run() {
            samples.addSnapshot(statics.createSnapshot());
        }
    }

    static class PerformRequestTask implements Callable {
        final AtomicLong totalRemain;
        final DLedgerClient dLedgerClient;
        final BenchmarkStatics statics;
        final int entrySize;

        public PerformRequestTask(final AtomicLong totalRemain,
                                  final DLedgerClient dLedgerClient,
                                  final BenchmarkStatics statics,
                                  final int entrySize) {
            this.totalRemain = totalRemain;
            this.dLedgerClient = dLedgerClient;
            this.statics = statics;
            this.entrySize = entrySize;
        }

        @Override
        public Object call() {
            while (totalRemain.decrementAndGet() > 0) {
                requestAndStatics(dLedgerClient, statics, entrySize);

            }
            return null;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        BenchmarkParameter benchmarkParameter = new BenchmarkParameter();
        JCommander.newBuilder().addObject(benchmarkParameter).build().parse(args);

        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(benchmarkParameter.getThreads());
        final DLedgerClient dLedgerClient = new DLedgerClient(benchmarkParameter.getGroup(), benchmarkParameter.getPeers());
        final Timer timer = new Timer("BenchmarkTimerThread", true);
        final BenchmarkStatics statics = new BenchmarkStatics();
        final BenchmarkStaticsSnapshots snapshots = new BenchmarkStaticsSnapshots(10);

        timer.scheduleAtFixedRate(new SnapshotTask(statics, snapshots), 100, 100);
        timer.scheduleAtFixedRate(new PrintTask(statics, snapshots), 1000, 1000);

        dLedgerClient.startup();

        final AtomicLong totalRemain = new AtomicLong(benchmarkParameter.counts);

        List<Callable<Void>> tasks = new ArrayList<>();
        IntStream.range(0, benchmarkParameter.getThreads()).forEach(i -> tasks.add(new PerformRequestTask(totalRemain, dLedgerClient, statics, benchmarkParameter.getEntrySize())));
        List<Future<Void>> futures = sendThreadPool.invokeAll(tasks);

        waitAllFinish(futures);
        timer.cancel();
        sendThreadPool.shutdown();
        dLedgerClient.shutdown();

        System.out.printf("Finished with total:%d, success:%d failed:%d, avg rt:%d\n",
                statics.sendRequestSuccessCount.get() + statics.receiveResponseFailedCount.get(),
                statics.sendRequestSuccessCount.get(),
                statics.sendRequestFailedCount.get(),
                statics.getSendMessageSuccessTimeTotal().get() / (statics.sendRequestSuccessCount.get() + statics.receiveResponseFailedCount.get()));

    }

    private static void waitAllFinish(List<Future<Void>> futures) {
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                //ignore
            }
        }
    }


    private static void requestAndStatics(DLedgerClient dLedgerClient, BenchmarkStatics statics, int entrySize) {
        try {
            final long beginTimestamp = System.currentTimeMillis();
            byte[] entry = mockEntry(entrySize);
            AppendEntryResponse response = dLedgerClient.append(entry);
            if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                statics.getSendRequestSuccessCount().incrementAndGet();
                statics.getReceiveResponseSuccessCount().incrementAndGet();
                final long currentRT = System.currentTimeMillis() - beginTimestamp;
                statics.getSendMessageSuccessTimeTotal().addAndGet(currentRT);
                long prevMaxRT = statics.getSendMessageMaxRT().get();
                while (currentRT > prevMaxRT) {
                    boolean updated = statics.getSendMessageMaxRT().compareAndSet(prevMaxRT, currentRT);
                    if (updated)
                        break;

                    prevMaxRT = statics.getSendMessageMaxRT().get();
                }
            } else {
                statics.getReceiveResponseFailedCount().incrementAndGet();
            }
        } catch (Exception e) {
            statics.getSendRequestFailedCount().incrementAndGet();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ignored) {
            }
        }
    }


    private static byte[] mockEntry(int size) {
        byte[] array = new byte[size];
        new Random().nextBytes(array);
        return array;
    }
}
