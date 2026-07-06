package handler;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import metadata.MetadataStore;
import response.FetchResponse;
import response.Response;
import server.RequestHeader;
import util.Bytes;

public class FetchHandler implements ApiHandler {

    private MetadataStore store;

    public FetchHandler(MetadataStore store) {
        this.store = store;
    }

    @Override
    public Response handle(RequestHeader header, DataInputStream in) throws IOException {
        short clientIdLength = in.readShort();
        in.skipBytes(clientIdLength);
        in.skipBytes(1);

        in.skipBytes(21);
        int topicCount = in.readByte() - 1;

        List<UUID> requestedTopicIds = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            byte[] uuidBytes = new byte[16];
            in.readFully(uuidBytes);
            requestedTopicIds.add(Bytes.toUUID(uuidBytes));
        }

        List<FetchResponse.Topic> responses = new ArrayList<>();
        for (UUID topicId : requestedTopicIds) {
            String topicName = store.topicName(topicId);
            boolean exists = topicName != null;

            short errorCode = exists ? (short) 0 : (short) 100;
            byte[] records = null;

            if (exists) {
                records = readPartitionLog(topicName, 0);
            }
            responses.add(FetchResponse.Topic.builder()
                    .topicId(topicId)
                    .partitions(List.of(
                            FetchResponse.Partition.builder()
                                    .partitionIndex(0)
                                    .errorCode(errorCode)
                                    .highWatermark(0)
                                    .lastStableOffset(0)
                                    .logStartOffset(0)
                                    .abortedTransactions(List.of())
                                    .preferredReadReplica(0)
                                    .records(records)
                                    .build()
                    ))
                    .build());
        }

        return FetchResponse.builder()
                .correlationId(header.correlationId())
                .throttleTimeMs(0)
                .errorCode((short) 0)
                .sessionId(0)
                .responses(responses)
                .build();
    }

    private static byte[] readPartitionLog(String topicName, int partition) {
        Path path = Path.of("/tmp/kraft-combined-logs/"
                + topicName + "-" + partition
                + "/00000000000000000000.log");
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            return null;
        }
    }
}
