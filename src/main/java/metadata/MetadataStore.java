package metadata;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import metadata.MetadataRecord.PartitionRecord;

public class MetadataStore {

    private final Map<String, byte[]> topicIdByName;
    private final Map<UUID, String> nameByTopicId;
    private final Map<UUID, List<PartitionRecord>> partitionsByTopicId;

    private MetadataStore(Map<String, byte[]> topicIdByName, Map<UUID, String> nameByTopicId,
                          Map<UUID, List<PartitionRecord>> partitionsByTopicId) {
        this.topicIdByName = topicIdByName;
        this.nameByTopicId = nameByTopicId;
        this.partitionsByTopicId = partitionsByTopicId;
    }

    public static MetadataStore load(String logPath) throws IOException {
        Map<String, byte[]> topicIdByName = new HashMap<>();
        Map<UUID, String> nameByTopicId = new HashMap<>();
        Map<UUID, List<PartitionRecord>> partitionsByTopicId = new HashMap<>();

        List<RecordBatch> batches = new ArrayList<>();
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(new FileInputStream(logPath)))) {
            while (true) {
                try {
                    batches.add(RecordBatch.from(input));
                } catch (EOFException e) {
                    break;
                }
            }
        }

        for (RecordBatch batch : batches) {
            for (RecordBatch.Record record : batch.records()) {
                if (record.value() == null) {
                    continue;
                }
                try {
                    MetadataRecord meta = MetadataRecord.from(record.value());
                    if (meta instanceof MetadataRecord.TopicRecord(String name, byte[] topicId)) {
                        topicIdByName.put(name, topicId);
                        nameByTopicId.put(toUUID(topicId), name);
                    } else if (meta instanceof MetadataRecord.PartitionRecord p) {
                        UUID key = toUUID(p.topicId());
                        partitionsByTopicId.computeIfAbsent(key, k -> new ArrayList<>()).add(p);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("Error parsing record: " + Arrays.toString(record.value()));
                }
            }
        }

        return new MetadataStore(topicIdByName, nameByTopicId, partitionsByTopicId);
    }

    public byte[] topicId(String topicName) {
        return topicIdByName.get(topicName);
    }

    public boolean topicExists(String topicName) {
        return topicIdByName.containsKey(topicName);
    }

    public String topicName(UUID id) {
        return nameByTopicId.get(id);
    }

    public List<PartitionRecord> partitions(byte[] topicId) {
        return partitionsByTopicId.getOrDefault(toUUID(topicId), List.of());
    }

    private static UUID toUUID(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low);
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
