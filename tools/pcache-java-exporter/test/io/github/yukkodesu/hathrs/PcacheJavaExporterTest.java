package io.github.yukkodesu.hathrs;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Hashtable;

public final class PcacheJavaExporterTest {
    private static final int LRU_CACHE_SIZE = 1048576;

    public static void main(String[] args) throws Exception {
        Path dataDir = Files.createTempDirectory("pcache-java-exporter-test");
        try {
            Hashtable<String, Long> ages = new Hashtable<String, Long>();
            ages.put("a3f0", 1700000000000L);
            short[] lru = new short[LRU_CACHE_SIZE];
            lru[123] = (short) 0x8000;
            lru[456] = (short) 0xffff;
            writeObject(dataDir.resolve("pcache_ages"), ages);
            writeObject(dataDir.resolve("pcache_lru"), lru);
            Files.write(dataDir.resolve("pcache_info"), Arrays.asList(
                "cacheCount=7",
                "cacheSize=99",
                "lruClearPointer=17",
                "agesHash=" + sha1Hex(Files.readAllBytes(dataDir.resolve("pcache_ages"))),
                "lruHash=" + sha1Hex(Files.readAllBytes(dataDir.resolve("pcache_lru")))
            ));

            ByteArrayOutputStream output = new ByteArrayOutputStream();
            PcacheJavaExporter.export(dataDir, output);
            byte[] bytes = output.toByteArray();
            assertTrue(bytes.length == 48 + LRU_CACHE_SIZE * 2 + 32, "unexpected output size");
            assertTrue(Arrays.equals(Arrays.copyOfRange(bytes, 0, 8), "HATHPC01".getBytes("US-ASCII")), "bad magic");
            ByteBuffer data = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            assertTrue(data.getShort(8) == 1, "bad version");
            assertTrue((data.getShort(48 + 123 * 2) & 0xffff) == 0x8000, "high LRU bit lost");
            assertTrue((data.getShort(48 + 456 * 2) & 0xffff) == 0xffff, "all LRU bits lost");
            byte[] expectedDigest = MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(bytes, bytes.length - 32));
            assertTrue(Arrays.equals(expectedDigest, Arrays.copyOfRange(bytes, bytes.length - 32, bytes.length)), "bad digest");

            Files.write(dataDir.resolve("pcache_ages"), new byte[] {0});
            try {
                PcacheJavaExporter.export(dataDir, new ByteArrayOutputStream());
                throw new AssertionError("corrupt source was accepted");
            } catch (java.io.IOException expected) {
                assertTrue(expected.getMessage().contains("pcache_ages"), "wrong corruption error");
            }
        } finally {
            deleteTree(dataDir);
        }
    }

    private static void writeObject(Path path, Object value) throws Exception {
        ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(path.toFile()));
        stream.writeObject(value);
        stream.close();
    }

    private static String sha1Hex(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-1").digest(bytes);
        StringBuilder result = new StringBuilder(40);
        for (byte value : digest) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }

    private static void assertTrue(boolean value, String message) {
        if (!value) {
            throw new AssertionError(message);
        }
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        Files.walk(root)
            .sorted(java.util.Comparator.reverseOrder())
            .forEach(path -> {
                try {
                    Files.delete(path);
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            });
    }
}
