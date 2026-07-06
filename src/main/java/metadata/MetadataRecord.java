package metadata;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import util.Decoder;

public sealed interface MetadataRecord
        permits MetadataRecord.TopicRecord,
                MetadataRecord.PartitionRecord {

    int TYPE_TOPIC = 2;
    int TYPE_PARTITION = 3;

    static MetadataRecord from(byte[] value) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(value));

        byte frameVersion = in.readByte();
        byte type = in.readByte();
        byte version = in.readByte();

        return switch (type) {
            case TYPE_TOPIC -> TopicRecord.parse(in);
            case TYPE_PARTITION -> PartitionRecord.parse(in);
            default -> throw new IOException("Unknown metadata record type: " + type);
        };
    }

    record TopicRecord(
            String name,
            byte[] topicId
    ) implements MetadataRecord {
        static TopicRecord parse(DataInputStream in) throws IOException {
            int nameLength = Decoder.readUnsignedVarInt(in) - 1;
            byte[] nameBytes = new byte[nameLength];
            in.readFully(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);

            byte[] topicId = new byte[16];
            in.readFully(topicId);

            return new TopicRecord(name, topicId);
        }
    }

    record PartitionRecord(
            int partitionId,
            byte[] topicId,
            List<Integer> replicaNodes,
            List<Integer> isrNodes,
            int leaderId,
            int leaderEpoch
    ) implements MetadataRecord {
        static PartitionRecord parse(DataInputStream in) throws IOException {
            int partitionId = in.readInt();

            byte[] topicId = new byte[16];
            in.readFully(topicId);

            List<Integer> replicaNodes = readIntCompactArray(in);
            List<Integer> isrNodes = readIntCompactArray(in);
            readIntCompactArray(in);
            readIntCompactArray(in);

            int leaderId = in.readInt();
            int leaderEpoch = in.readInt();
            return new PartitionRecord(
                    partitionId, topicId, replicaNodes, isrNodes, leaderId, leaderEpoch
            );
        }

        private static List<Integer> readIntCompactArray(DataInputStream in) throws IOException {
            int count = Decoder.readUnsignedVarInt(in) - 1; // compact array
            List<Integer> result = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                result.add(in.readInt());
            }
            return result;
        }
    }
}
