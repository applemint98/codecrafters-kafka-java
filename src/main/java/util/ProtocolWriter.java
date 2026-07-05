package util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public final class ProtocolWriter {

    private ProtocolWriter() {
        throw new AssertionError("Utility class");
    }

    public static void writeIntCompactArray(DataOutputStream out, List<Integer> arr) throws IOException {
        writeCompactArray(out, arr, DataOutputStream::writeInt);
    }

    public static <T> void writeCompactArray(DataOutputStream out, List<T> arr, ElementWriter<T> writer)
            throws IOException {
        writeCompactArrayLength(out, arr.size());
        for (T v : arr) {
            writer.write(out, v);
        }
    }

    public static void writeCompactString(DataOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeByte(bytes.length + 1);
        out.write(bytes);
    }

    public static void writeEmptyTagBuffer(DataOutputStream out) throws IOException {
        out.writeByte(0);
    }

    public static void writeUUID(DataOutputStream out, UUID uuid) throws IOException {
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
    }

    public static void writeCompactBytes(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            out.writeByte(0);
        } else {
            writeUnsignedVarInt(out, bytes.length + 1);
            out.write(bytes);
        }
    }

    private static void writeUnsignedVarInt(DataOutputStream out, int value) throws IOException {
        while ((value & ~0x7F) != 0) {
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value);
    }

    private static void writeCompactArrayLength(DataOutputStream out, int size) throws IOException {
        out.writeByte(size + 1);
    }

    @FunctionalInterface
    public interface ElementWriter<T> {
        void write(DataOutputStream out, T value) throws IOException;
    }
}
