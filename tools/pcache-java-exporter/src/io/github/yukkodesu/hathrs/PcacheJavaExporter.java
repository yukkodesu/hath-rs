package io.github.yukkodesu.hathrs;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.DigestOutputStream;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public final class PcacheJavaExporter {
    private static final int LRU_CACHE_SIZE = 1048576;

    private PcacheJavaExporter() {}

    public static void main(String[] args) {
        if (args.length == 1 && "--help".equals(args[0])) {
            System.out.println("Usage: pcache-java-exporter.jar --data-dir <DIR>");
            return;
        }
        if (args.length != 2 || !"--data-dir".equals(args[0])) {
            System.err.println("Usage: pcache-java-exporter.jar --data-dir <DIR>");
            System.exit(2);
        }
        try {
            export(java.nio.file.Paths.get(args[1]), System.out);
        } catch (Exception exception) {
            System.err.println("Migration export failed: " + exception.getMessage());
            System.exit(1);
        }
    }

    static void export(Path dataDir, OutputStream rawOutput) throws Exception {
        Map<String, String> info = readInfo(dataDir.resolve("pcache_info"));
        int cacheCount = parseNonNegativeInt(info.get("cacheCount"), "cacheCount");
        long cacheSize = parseNonNegativeLong(info.get("cacheSize"), "cacheSize");
        int lruClearPointer = parseNonNegativeInt(info.get("lruClearPointer"), "lruClearPointer");
        if (lruClearPointer >= LRU_CACHE_SIZE) {
            throw new IOException("invalid lruClearPointer");
        }

        Path agesPath = dataDir.resolve("pcache_ages");
        Path lruPath = dataDir.resolve("pcache_lru");
        verifySha1(agesPath, info.get("agesHash"));
        verifySha1(lruPath, info.get("lruHash"));
        Map<String, Long> ages = readAges(agesPath);
        short[] lru = readLru(lruPath);

        DigestOutputStream digesting = new DigestOutputStream(
            rawOutput, MessageDigest.getInstance("SHA-256"));
        DataOutputStream output = new DataOutputStream(digesting);
        output.write("HATHPC01".getBytes(StandardCharsets.US_ASCII));
        writeShortLE(output, 1);
        writeIntLE(output, cacheCount);
        writeLongLE(output, cacheSize);
        writeIntLE(output, lruClearPointer);
        writeIntLE(output, ages.size());
        for (Map.Entry<String, Long> entry : ages.entrySet()) {
            byte[] range = entry.getKey().getBytes(StandardCharsets.US_ASCII);
            writeShortLE(output, range.length);
            output.write(range);
            writeLongLE(output, entry.getValue().longValue());
        }
        writeIntLE(output, lru.length);
        for (short value : lru) {
            writeShortLE(output, value & 0xffff);
        }
        output.flush();
        rawOutput.write(digesting.getMessageDigest().digest());
        rawOutput.flush();
    }

    private static Map<String, String> readInfo(Path infoPath) throws IOException {
        Map<String, String> result = new HashMap<String, String>();
        for (String line : Files.readAllLines(infoPath, StandardCharsets.UTF_8)) {
            int separator = line.indexOf('=');
            if (separator <= 0 || result.put(line.substring(0, separator), line.substring(separator + 1)) != null) {
                throw new IOException("invalid pcache_info");
            }
        }
        for (String key : new String[] {"cacheCount", "cacheSize", "lruClearPointer", "agesHash", "lruHash"}) {
            if (!result.containsKey(key)) {
                throw new IOException("pcache_info missing " + key);
            }
        }
        return result;
    }

    private static void verifySha1(Path path, String expected) throws Exception {
        if (expected == null || !expected.matches("[0-9a-f]{40}")) {
            throw new IOException("invalid SHA-1 for " + path.getFileName());
        }
        String actual = hex(MessageDigest.getInstance("SHA-1").digest(Files.readAllBytes(path)));
        if (!expected.equals(actual)) {
            throw new IOException("SHA-1 mismatch for " + path.getFileName());
        }
    }

    private static Map<String, Long> readAges(Path path) throws Exception {
        Object value = readObject(path);
        if (!(value instanceof Hashtable)) {
            throw new IOException("pcache_ages is not a Hashtable");
        }
        Map<String, Long> result = new HashMap<String, Long>();
        for (Object rawEntry : ((Hashtable<?, ?>) value).entrySet()) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) rawEntry;
            if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof Long)) {
                throw new IOException("pcache_ages has an unsupported entry type");
            }
            String range = (String) entry.getKey();
            long timestamp = ((Long) entry.getValue()).longValue();
            if (!range.matches("[0-9a-f]{4}") || timestamp < 0 || result.put(range, timestamp) != null) {
                throw new IOException("pcache_ages has an invalid range entry");
            }
        }
        if (result.size() > 65536) {
            throw new IOException("pcache_ages has too many ranges");
        }
        return result;
    }

    private static short[] readLru(Path path) throws Exception {
        Object value = readObject(path);
        if (!(value instanceof short[]) || ((short[]) value).length != LRU_CACHE_SIZE) {
            throw new IOException("pcache_lru has an invalid length");
        }
        return (short[]) value;
    }

    private static Object readObject(Path path) throws Exception {
        InputStream file = new FileInputStream(path.toFile());
        SafeObjectInputStream input = new SafeObjectInputStream(file);
        try {
            return input.readObject();
        } finally {
            input.close();
        }
    }

    private static int parseNonNegativeInt(String value, String field) throws IOException {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) throw new NumberFormatException();
            return parsed;
        } catch (Exception exception) {
            throw new IOException("invalid " + field);
        }
    }

    private static long parseNonNegativeLong(String value, String field) throws IOException {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) throw new NumberFormatException();
            return parsed;
        } catch (Exception exception) {
            throw new IOException("invalid " + field);
        }
    }

    private static void writeShortLE(DataOutputStream output, int value) throws IOException {
        output.writeShort(Short.reverseBytes((short) value));
    }

    private static void writeIntLE(DataOutputStream output, int value) throws IOException {
        output.writeInt(Integer.reverseBytes(value));
    }

    private static void writeLongLE(DataOutputStream output, long value) throws IOException {
        output.writeLong(Long.reverseBytes(value));
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }

    private static final class SafeObjectInputStream extends ObjectInputStream {
        SafeObjectInputStream(InputStream input) throws IOException {
            super(input);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass descriptor) throws IOException, ClassNotFoundException {
            String name = descriptor.getName();
            if (!name.equals("java.util.Hashtable") && !name.equals("java.lang.Long")
                    && !name.equals("java.lang.Number") && !name.equals("[S")) {
                throw new InvalidClassException("unsupported serialized class", name);
            }
            return super.resolveClass(descriptor);
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
            throw new InvalidClassException("serialized proxies are not allowed");
        }
    }
}
