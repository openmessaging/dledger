package io.openmessaging.storage.dleger.util;

import java.io.File;
import java.util.UUID;
import io.openmessaging.storage.dleger.utils.IOUtils;

public class FileTestUtil {

    public static final String TEST_BASE = File.separator + "tmp" + File.separator + "dlegerteststore";

    public static String createTestDir() {
        return createTestDir(null);
    }

    public static String createTestDir(String prefix) {
        if (prefix == null) {
            prefix = "test";
        }
        String baseDir =  TEST_BASE + File.separator + prefix + "-" + UUID.randomUUID().toString();
        final File file = new File(baseDir);
        if (file.exists()) {
            System.exit(1);
        }
        return baseDir;
    }

    public static void deleteFile(String fileName) {
        IOUtils.deleteFile(new File(fileName));
    }

    public static void deleteFile(File file) {
        IOUtils.deleteFile(file);
    }

}
