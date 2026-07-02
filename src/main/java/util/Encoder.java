package util;

import java.io.DataOutputStream;
import java.io.IOException;

public final class Encoder {

    private Encoder() {
        throw new AssertionError("Utility class");
    }

    public static void writeUnsignedVarInt(int value, DataOutputStream out) throws IOException {
        while ((value & ~0x7F) != 0) {
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value);
    }

    public static void writeSingedVarInt(int value, DataOutputStream out) throws IOException {
        int zigzag = (value << 1) ^ (value >> 31);
        writeUnsignedVarInt(zigzag, out);
    }
}
