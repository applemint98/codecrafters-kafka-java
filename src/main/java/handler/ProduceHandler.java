package handler;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import metadata.MetadataRecord.PartitionRecord;
import metadata.MetadataStore;
import response.ProduceResponse;
import response.Response;
import server.RequestHeader;
import util.Decoder;

public class ProduceHandler implements ApiHandler {

    private MetadataStore store;

    public ProduceHandler(MetadataStore store) {
        this.store = store;
    }

    @Override
    public Response handle(RequestHeader header, DataInputStream in) throws IOException {
        short clientIdLength = in.readShort();
        in.skipBytes(clientIdLength);
        in.skipBytes(1);

        int txnIdLen = Decoder.readUnsignedVarInt(in);
        if (txnIdLen > 0) {
            in.skipBytes(txnIdLen - 1);
        }

        in.skipBytes(2);
        in.skipBytes(4);

        int topicCount = Decoder.readUnsignedVarInt(in) - 1;

        // Response

        List<ProduceResponse.Topic> topics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            int nameLen = Decoder.readUnsignedVarInt(in) - 1;
            byte[] nameBytes = new byte[nameLen];
            in.readFully(nameBytes);
            String topicName = new String(nameBytes, StandardCharsets.UTF_8);

            byte[] topicIdBytes = store.topicId(topicName);
            boolean topicExists = topicIdBytes != null;

            boolean partitionExists = false;

            List<ProduceResponse.Partition> partitions = new ArrayList<>();
            int partitionCount = Decoder.readUnsignedVarInt(in) - 1;
            for (int j = 0; j < partitionCount; j++) {
                int partitionIndex = in.readInt();
                int recordsLen = Decoder.readUnsignedVarInt(in);
                byte[] recordBatchBytes = null;
                if (recordsLen > 0) {
                    recordBatchBytes = new byte[recordsLen - 1];
                    in.readFully(recordBatchBytes);
                }
                in.skipBytes(1);

                if (topicExists) {
                    List<PartitionRecord> parts = store.partitions(topicIdBytes);
                    partitionExists = parts.stream()
                            .anyMatch(p -> p.partitionId() == partitionIndex);
                }
                boolean valid = topicExists && partitionExists;

                short errorCode;
                long baseOffset, logStartOffset;

                if (valid) {
                    errorCode = 0;
                    baseOffset = 0;
                    logStartOffset = 0;
                } else {
                    errorCode = 3;
                    baseOffset = -1;
                    logStartOffset = -1;
                }

                ProduceResponse.Partition partition = ProduceResponse.Partition.builder()
                        .index(partitionIndex)
                        .errorCode(errorCode)
                        .baseOffset(baseOffset)
                        .logAppendTimeMs(-1L)
                        .logStartOffset(logStartOffset)
                        .build();
                partitions.add(partition);

                if (valid && recordBatchBytes != null) {
                    Path path = Path.of("/tmp/kraft-combined-logs/"
                            + topicName + "-" + partitionIndex
                            + "/00000000000000000000.log");
                    Files.createDirectories(path.getParent());
                    Files.write(path, recordBatchBytes,
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
            }

            in.skipBytes(1);

            ProduceResponse.Topic topic = ProduceResponse.Topic.builder()
                    .name(topicName)
                    .partitions(partitions)
                    .build();
            topics.add(topic);
        }
        in.skipBytes(1);

        return ProduceResponse.builder()
                .correlationId(header.correlationId())
                .topics(topics)
                .throttleTime(0)
                .build();
    }
}
