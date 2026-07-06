package util;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class Bytes {

    private Bytes() {
        throw new AssertionError("Utility class");
    }

    public static UUID toUUID(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low);
    }
}
