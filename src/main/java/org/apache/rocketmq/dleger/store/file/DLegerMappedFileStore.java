package org.apache.rocketmq.dleger.store.file;

import java.nio.ByteBuffer;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.entry.DLegerEntryCoder;
import org.apache.rocketmq.dleger.exception.DLegerException;
import org.apache.rocketmq.dleger.store.DLegerStore;
import org.apache.rocketmq.dleger.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerMappedFileStore extends DLegerStore {

    private static Logger logger = LoggerFactory.getLogger(DLegerMappedFileStore.class);

    private static final int INDEX_NUIT_SIZE = 32;

    private long legerEndIndex = -1;
    private long committedIndex = -1;
    private long legerEndTerm;

    private DLegerConfig dLegerConfig;
    private MemberState memberState;

    private MappedFileQueue dataFileQueue;
    private MappedFileQueue indexFileQueue;

    private ThreadLocal<ByteBuffer> localEntryBuffer;
    private ThreadLocal<ByteBuffer> localIndexBuffer;


    public DLegerMappedFileStore(DLegerConfig dLegerConfig, MemberState memberState) {
        this.dLegerConfig = dLegerConfig;
        this.memberState = memberState;
        this.dataFileQueue = new MappedFileQueue(dLegerConfig.getDataStorePath(), dLegerConfig.getMappedFileSizeForEntryData());
        this.indexFileQueue = new MappedFileQueue(dLegerConfig.getIndexStorePath(), dLegerConfig.getMappedFileSizeForEntryIndex());
        localEntryBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(4 * 1024 * 1024));
        localIndexBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(INDEX_NUIT_SIZE * 2));
    }


    public void startup() {
        this.dataFileQueue.load();
        this.indexFileQueue.load();
        //recover
    }

    public void shutdown() {
        this.dataFileQueue.flush(0);
        this.indexFileQueue.flush(0);
    }


    @Override
    public long appendAsLeader(DLegerEntry entry) {
        PreConditions.check(memberState.isLeader(), DLegerException.Code.NOT_LEADER, null, memberState.getLeaderId());
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLegerEntryCoder.encode(entry, dataBuffer);
        int entrySize =  dataBuffer.remaining();
        synchronized (memberState) {
            //TODO handle disk  error
            long nextIndex = legerEndIndex + 1;
            PreConditions.check(memberState.isLeader(), DLegerException.Code.NOT_LEADER, null, memberState.getLeaderId());
            entry.setIndex(nextIndex);
            entry.setTerm(memberState.currTerm());
            DLegerEntryCoder.setIndexTerm(dataBuffer, nextIndex, memberState.currTerm(), entry.getMagic());
            long dataPos = dataFileQueue.append(dataBuffer.array(), 0, dataBuffer.remaining());
            PreConditions.check(dataPos != -1, DLegerException.Code.DISK_ERROR, null);
            DLegerEntryCoder.encodeIndex(dataPos, entrySize, 0, nextIndex, memberState.currTerm(),indexBuffer);
            long indexPos = indexFileQueue.append(indexBuffer.array(), 0, indexBuffer.remaining());
            PreConditions.check(indexPos != -1, DLegerException.Code.DISK_ERROR, null);
            logger.info("[{}] Append as Leader {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            legerEndIndex++;
            committedIndex++;
            legerEndTerm = memberState.currTerm();
            return legerEndIndex;
        }
    }


    @Override
    public long appendAsFollower(DLegerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLegerException.Code.NOT_FOLLOWER, null, memberState.getLeaderId());
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLegerEntryCoder.encode(entry, dataBuffer);
        int entrySize = dataBuffer.remaining();
        synchronized(memberState) {
            long nextIndex = legerEndIndex + 1;
            PreConditions.check(nextIndex ==  entry.getIndex(), DLegerException.Code.UNCONSISTENCT_INDEX, null, memberState.getLeaderId());
            PreConditions.check(memberState.isFollower(), DLegerException.Code.NOT_FOLLOWER, null, memberState.getLeaderId());
            PreConditions.check(leaderTerm == memberState.currTerm(), DLegerException.Code.UNCONSISTENCT_TERM, null, memberState.getLeaderId());
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLegerException.Code.UNCONSISTENCT_LEADER, null, memberState.getLeaderId());
            long dataPos = dataFileQueue.append(dataBuffer.array(), 0, dataBuffer.remaining());
            PreConditions.check(dataPos != -1, DLegerException.Code.DISK_ERROR, null);
            DLegerEntryCoder.encodeIndex(dataPos, entrySize, 0, entry.getIndex(), memberState.currTerm(),indexBuffer);
            long indexPos = indexFileQueue.append(indexBuffer.array(), 0, indexBuffer.remaining());
            PreConditions.check(indexPos != -1, DLegerException.Code.DISK_ERROR, null);
            legerEndTerm = memberState.currTerm();
            legerEndIndex = entry.getIndex();
            committedIndex = entry.getIndex();
            return entry.getIndex();
        }

    }

    public long getLegerEndIndex() {
        return legerEndIndex;
    }


    @Override
    public DLegerEntry get(Long index) {
        PreConditions.check(index <= legerEndIndex, DLegerException.Code.INDEX_OUT_OF_RANGE, null, memberState.getLeaderId());
        SelectMappedBufferResult indexSbr = indexFileQueue.getData(index * INDEX_NUIT_SIZE, INDEX_NUIT_SIZE);
        PreConditions.check(indexSbr.getByteBuffer() != null, DLegerException.Code.DISK_ERROR, null);
        long pos = indexSbr.getByteBuffer().getLong();
        int size = indexSbr.getByteBuffer().getInt();
        indexSbr.release();
        SelectMappedBufferResult dataSbr = dataFileQueue.getData(pos, size);
        PreConditions.check(dataSbr.getByteBuffer() != null, DLegerException.Code.DISK_ERROR, null);
        DLegerEntry dLegerEntry = DLegerEntryCoder.decode(dataSbr.getByteBuffer());
        dataSbr.release();
        return dLegerEntry;
    }

    public long getCommittedIndex() {
        return committedIndex;
    }

    public long getLegerEndTerm() {
        return legerEndTerm;
    }
}
