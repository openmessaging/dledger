package org.apache.rocketmq.dleger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import java.nio.ByteBuffer;
import org.apache.rocketmq.dleger.client.DLegerClient;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.entry.DLegerEntryCoder;
import org.apache.rocketmq.dleger.protocol.GetEntriesResponse;
import org.apache.rocketmq.dleger.store.file.DLegerMmapFileStore;
import org.apache.rocketmq.dleger.store.file.MmapFile;
import org.apache.rocketmq.dleger.store.file.MmapFileList;
import org.apache.rocketmq.dleger.store.file.SelectMmapBufferResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFileCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(ReadFileCommand.class);

    @Parameter(names = {"--dir", "-d"}, description = "the data dir")
    private String dataDir = null;


    @Parameter(names = {"--pos", "-p"}, description = "the start pos")
    private long pos = 0;

    @Parameter(names = {"--size", "-s"}, description = "the file size")
    private int size = -1;

    @Parameter(names = {"--index", "-i"}, description = "the index")
    private long index = -1L;


    @Parameter(names = {"--body", "-b"}, description = "if read the body")
    private boolean readBody = false;


    @Override
    public void doCommand() {
        if (index != -1) {
            pos = index * DLegerMmapFileStore.INDEX_NUIT_SIZE;
            if (size == -1) {
                size = DLegerMmapFileStore.INDEX_NUIT_SIZE * 1024 * 1024;
            }
        } else {
            if (size == -1) {
                size = 1024 * 1024 * 1024;
            }
        }
        MmapFileList mmapFileList = new MmapFileList(dataDir, size);
        mmapFileList.load();
        MmapFile mmapFile = mmapFileList.findMappedFileByOffset(pos);
        if (mmapFile == null) {
            logger.info("Cannot find the file");
            return;
        }
        SelectMmapBufferResult result = mmapFile.selectMappedBuffer((int) (pos % size));
        ByteBuffer buffer = result.getByteBuffer();
        if (index != -1) {
            logger.info("magic={} pos={} size={} index={} term={}", buffer.getInt(), buffer.getLong(), buffer.getInt(), buffer.getLong(), buffer.getLong());
        } else {
            DLegerEntry entry = DLegerEntryCoder.decode(buffer, readBody);
            logger.info(JSON.toJSONString(entry));
        }
    }
}
