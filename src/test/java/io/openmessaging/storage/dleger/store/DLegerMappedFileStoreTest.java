package io.openmessaging.storage.dleger.store;

import io.openmessaging.storage.dleger.DLegerConfig;
import io.openmessaging.storage.dleger.MemberState;
import io.openmessaging.storage.dleger.ServerTestHarness;
import io.openmessaging.storage.dleger.store.file.DLegerMmapFileStore;
import io.openmessaging.storage.dleger.util.FileTestUtil;
import java.nio.ByteBuffer;
import java.util.UUID;
import io.openmessaging.storage.dleger.entry.DLegerEntry;
import org.junit.Assert;
import org.junit.Test;

import static io.openmessaging.storage.dleger.store.file.MmapFileList.MIN_BLANK_LEN;

public class DLegerMappedFileStoreTest extends ServerTestHarness {




    private synchronized DLegerMmapFileStore createFileStore(String group, String peers, String selfId, String leaderId) {
       return createFileStore(group, peers, selfId, leaderId, 10 * 1024 * 1024, DLegerMmapFileStore.INDEX_NUIT_SIZE * 1024 * 1024);
    }

    private synchronized DLegerMmapFileStore createFileStore(String group, String peers, String selfId, String leaderId,
        int dataFileSize, int indexFileSize) {
        DLegerConfig config = new DLegerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLegerConfig.MEMORY);
        config.setDiskSpaceRatioToForceClean(0.90f);
        config.setEnableDiskForceClean(false);
        config.setEnableLeaderElector(false);
        if (dataFileSize != -1) {
            config.setMappedFileSizeForEntryData(dataFileSize);
        }
        if (indexFileSize != -1) {
            config.setMappedFileSizeForEntryIndex(indexFileSize);
        }

        MemberState memberState = new MemberState(config);
        memberState.setCurrTermForTest(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        bases.add(config.getDataStorePath());
        bases.add(config.getIndexStorePath());
        bases.add(config.getDefaultPath());
        DLegerMmapFileStore fileStore  = new DLegerMmapFileStore(config, memberState);
        fileStore.startup();
        return fileStore;
    }


    @Test
    public void testAppendHook() throws Exception {
        String group = UUID.randomUUID().toString();
        DLegerMmapFileStore fileStore =  createFileStore(group,  String.format("n0-localhost:%d", nextPort()), "n0", "n0");
        DLegerMmapFileStore.AppendHook appendHook = (entry, buffer, bodyOffset) -> {
            buffer.position(bodyOffset);
            buffer.putLong(entry.getIndex());
        };
        fileStore.addAppendHook(appendHook);
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setBody((new byte[128]));
            DLegerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        Assert.assertEquals(9, fileStore.getLegerEndIndex());
        for (long i = 0; i < 10; i++) {
            DLegerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertEquals(entry.getIndex(), ByteBuffer.wrap(entry.getBody()).getLong());
        }
    }

    @Test
    public void testAppendAsLeader() throws Exception {
        String group = UUID.randomUUID().toString();
        DLegerMmapFileStore fileStore =  createFileStore(group,  "n0-localhost:20911", "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setBody(("Hello Leader" + i).getBytes());
            DLegerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            DLegerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Leader" +i).getBytes(), entry.getBody());
        }

        for (long i = 0; i < 10; i++) {
            fileStore.updateCommittedIndex(0, i);
            Assert.assertEquals( i, fileStore.getCommittedIndex());
            DLegerEntry entry = fileStore.get( i);
            Assert.assertEquals(entry.getPos() + entry.getSize(), fileStore.getCommittedPos());
        }
        Assert.assertEquals(fileStore.getCommittedPos(), fileStore.getDataFileList().getMaxWrotePosition());

        //ignore the smaller index and smaller term
        fileStore.updateCommittedIndex(0, -1);
        Assert.assertEquals(9, fileStore.getLegerEndIndex());
        fileStore.updateCommittedIndex(0, 0);
        Assert.assertEquals(9, fileStore.getLegerEndIndex());
        fileStore.updateCommittedIndex(-1, 10);
        Assert.assertEquals(9, fileStore.getLegerEndIndex());
    }


    @Test
    public void testRecovery() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLegerMmapFileStore fileStore =  createFileStore(group,  peers, "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setBody(("Hello Leader With Recovery" + i).getBytes());
            DLegerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        while (fileStore.getFlushPos() != fileStore.getWritePos()) {
            fileStore.flush();
        }
        fileStore.shutdown();
        fileStore = createFileStore(group,  peers, "n0", "n0");
        Assert.assertEquals(0, fileStore.getLegerBeginIndex());
        Assert.assertEquals(9, fileStore.getLegerEndIndex());
        for (long i = 0; i < 10; i++) {
            DLegerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Leader With Recovery" +i).getBytes(), entry.getBody());
        }
    }


    @Test
    public void testTruncate() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLegerMmapFileStore fileStore =  createFileStore(group,  peers, "n0", "n0", 8 * 1024 + MIN_BLANK_LEN, 8 * DLegerMmapFileStore.INDEX_NUIT_SIZE);
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setBody(new byte[1024]);
            DLegerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        Assert.assertEquals(2, fileStore.getDataFileList().getMappedFiles().size());
        Assert.assertEquals(0, fileStore.getLegerBeginIndex());
        Assert.assertEquals(9, fileStore.getLegerEndIndex());
        fileStore.getMemberState().changeToFollower(fileStore.getLegerEndTerm(), "n0");


        DLegerMmapFileStore otherFileStore =  createFileStore(group,  peers, "n0", "n0", 8 * 1024 + MIN_BLANK_LEN, 8 * DLegerMmapFileStore.INDEX_NUIT_SIZE);

        {
            //truncate the mid
            DLegerEntry midEntry = otherFileStore.get(5L);
            Assert.assertNotNull(midEntry);
            long midIndex = fileStore.truncate(midEntry, fileStore.getLegerEndTerm(), "n0");
            Assert.assertEquals(5, midIndex);
            Assert.assertEquals(0, fileStore.getLegerBeginIndex());
            Assert.assertEquals(5, fileStore.getLegerEndIndex());
            Assert.assertEquals(midEntry.getPos() + midEntry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assert.assertEquals((midIndex + 1) * DLegerMmapFileStore.INDEX_NUIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }
        {
            //truncate just after
            DLegerEntry afterEntry = otherFileStore.get(6L);
            Assert.assertNotNull(afterEntry);
            long afterIndex = fileStore.truncate(afterEntry, fileStore.getLegerEndTerm(), "n0");
            Assert.assertEquals(6, afterIndex);
            Assert.assertEquals(0, fileStore.getLegerBeginIndex());
            Assert.assertEquals(6, fileStore.getLegerEndIndex());
            Assert.assertEquals(afterEntry.getPos() + afterEntry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assert.assertEquals((afterIndex + 1) * DLegerMmapFileStore.INDEX_NUIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }

        {
            //truncate to the end
            DLegerEntry endEntry = otherFileStore.get(9L);
            Assert.assertNotNull(endEntry);
            long endIndex = fileStore.truncate(endEntry, fileStore.getLegerEndTerm(), "n0");
            Assert.assertEquals(9, endIndex);
            Assert.assertEquals(9, fileStore.getLegerEndIndex());
            Assert.assertEquals(9, fileStore.getLegerBeginIndex());
            Assert.assertEquals(endEntry.getPos() + endEntry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assert.assertEquals((endIndex + 1) * DLegerMmapFileStore.INDEX_NUIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }

    }


    @Test
    public void testAppendAsFollower() {
        DLegerMmapFileStore fileStore =  createFileStore(UUID.randomUUID().toString(),  "n0-localhost:20913", "n0", "n1");
        long currPos = 0;
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setTerm(0);
            entry.setIndex(i);
            entry.setBody(("Hello Follower" + i).getBytes());
            entry.setPos(currPos);
            DLegerEntry resEntry = fileStore.appendAsFollower(entry, 0, "n1");
            Assert.assertEquals(i, resEntry.getIndex());
            currPos = currPos + entry.computSizeInBytes();
        }
        for (long i = 0; i < 10; i++) {
            DLegerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Follower" +i).getBytes(), entry.getBody());
        }
    }

}
