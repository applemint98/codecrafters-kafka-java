package util;

import java.io.DataInputStream;
import java.io.IOException;

public final class Decoder {

    private Decoder() {
        throw new AssertionError("Utility class");
    }

    public static int readUnsignedVarInt(DataInputStream in) throws IOException {
        int result = 0;
        int shift = 0;
        while (true) {
            byte b = in.readByte();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }

    public static int readSignedVarInt(DataInputStream in) throws IOException {
        int raw = readUnsignedVarInt(in);
        return (raw >>> 1) ^ -(raw & 1);
    }
}
