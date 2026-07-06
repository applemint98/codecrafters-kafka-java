package handler;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import metadata.MetadataRecord.PartitionRecord;
import metadata.MetadataStore;
import response.DescribeTopicPartitionsResponse;
import response.DescribeTopicPartitionsResponse.Partition;
import response.DescribeTopicPartitionsResponse.Topic;
import response.Response;
import server.RequestHeader;

public class DescribeTopicPartitionsHandler implements ApiHandler {

    private final MetadataStore store;

    public DescribeTopicPartitionsHandler(MetadataStore store) {
        this.store = store;
    }

    @Override
    public Response handle(RequestHeader header, DataInputStream in) throws IOException {
        // Request
        short client_id_length = in.readShort();
        in.skipBytes(client_id_length);
        in.skipBytes(1); // TagBuffer

        List<String> topicNameStrings = new ArrayList<>();
        byte topicArrayLength = (byte) (in.readByte() - 1);
        for (int i = 0; i < topicArrayLength; i++) {
            byte topicNameLength = (byte) (in.readByte() - 1);
            byte[] topicNameBytes = new byte[topicNameLength];
            in.readFully(topicNameBytes);
            topicNameStrings.add(new String(topicNameBytes, StandardCharsets.UTF_8));
            in.skipBytes(1); // TagBuffer
        }

        int responsePartitionLimit = in.readInt();
        byte cursor = in.readByte();
        in.skipBytes(1); // TagBuffer

        // Response
        List<Topic> topics = new ArrayList<>();
        for (String name : topicNameStrings) {
            byte[] topicIdBytes = store.topicId(name);

            if (topicIdBytes == null) { // 없는거
                topics.add(Topic.builder()
                        .errorCode((short) 3)
                        .topicName(name)
                        .topicId(new UUID(0, 0))
                        .isInternal(false)
                        .partitions(List.of())
                        .topicAuthorizedOperations(0)
                        .build());
            } else {
                UUID topicId = toUUID(topicIdBytes);
                List<PartitionRecord> parts = store.partitions(topicIdBytes);
                List<Partition> partitions = new ArrayList<>();
                for (PartitionRecord part : parts) {
                    partitions.add(Partition.builder()
                            .errorCode((short) 0)
                            .partitionIndex(part.partitionId())
                            .leaderId(part.leaderId())
                            .leaderEpoch(part.leaderEpoch())
                            .replicaNodes(part.replicaNodes())
                            .isrNodes(part.isrNodes())
                            .build());
                }

                topics.add(Topic.builder()
                        .errorCode((short) 0)
                        .topicName(name)
                        .topicId(topicId)
                        .isInternal(false)
                        .partitions(partitions)
                        .topicAuthorizedOperations(0)
                        .build());
                topics.sort(Comparator.comparing(Topic::topicName));
            }
        }

        return DescribeTopicPartitionsResponse.builder()
                .correlationId(header.correlationId())
                .throttleTime(0)
                .topics(topics)
                .build();
    }

    private static UUID toUUID(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low);
    }
}
