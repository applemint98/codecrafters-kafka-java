package metadata;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import util.Decoder;

public record RecordBatch(
        long baseOffset,
        int batchLength,
        int partitionLeaderEpoch,
        byte magic,
        int crc,
        short attributes,
        int lastOffsetDelta,
        long baseTimestamp,
        long maxTimestamp,
        long producerId,
        short producerEpoch,
        int baseSequence,
        int recordsCount,
        List<Record> records
) {
    public static RecordBatch from(DataInputStream in) throws IOException {
        long baseOffset = in.readLong();
        int batchLength = in.readInt();
        int partitionLeaderEpoch = in.readInt();
        byte magic = in.readByte();
        int crc = in.readInt();
        short attributes = in.readShort();
        int lastOffsetDelta = in.readInt();
        long baseTimestamp = in.readLong();
        long maxTimestamp = in.readLong();
        long producerId = in.readLong();
        short producerEpoch = in.readShort();
        int baseSequence = in.readInt();
        int recordsCount = in.readInt();

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < recordsCount; i++) {
            records.add(Record.from(in));
        }

        return new RecordBatch(
                baseOffset, batchLength, partitionLeaderEpoch, magic, crc,
                attributes, lastOffsetDelta, baseTimestamp, maxTimestamp,
                producerId, producerEpoch, baseSequence, recordsCount, records
        );
    }

    public record Record(
            int length,
            byte attributes,
            int timestampDelta,
            int offsetDelta,
            int keyLength,
            byte[] key,
            int valueLength,
            byte[] value,
            int headersCount
    ) {
        public static Record from(DataInputStream in) throws IOException {
            int length = Decoder.readSignedVarInt(in);
            byte attributes = in.readByte();
            int timestampDelta = Decoder.readSignedVarInt(in);
            int offsetDelta = Decoder.readSignedVarInt(in);

            int keyLength = Decoder.readSignedVarInt(in);
            byte[] key = null;
            if (keyLength >= 0) {
                key = new byte[keyLength];
                in.readFully(key);
            }

            int valueLength = Decoder.readSignedVarInt(in);
            byte[] value = null;
            if (valueLength >= 0) {
                value = new byte[valueLength];
                in.readFully(value);
            }

            int headersCount = Decoder.readUnsignedVarInt(in);
            return new Record(
                    length, attributes, timestampDelta, offsetDelta,
                    keyLength, key, valueLength, value, headersCount
            );
        }
    }
}
