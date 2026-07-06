package server;

import java.io.DataInputStream;
import java.io.IOException;

public record RequestHeader(
        int messageSize,
        short apiKey,
        short apiVersion,
        int correlationId
) {

    public static RequestHeader from(DataInputStream in) throws IOException {
        return new RequestHeader(
                in.readInt(),
                in.readShort(),
                in.readShort(),
                in.readInt()
        );
    }
}
