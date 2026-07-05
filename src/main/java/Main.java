import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import metadata.MetadataRecord;
import metadata.MetadataRecord.PartitionRecord;
import metadata.RecordBatch;
import response.ApiVersionsResponse;
import response.DescribeTopicPartitionsResponse;
import response.DescribeTopicPartitionsResponse.Partition;
import response.DescribeTopicPartitionsResponse.Topic;
import response.FetchResponse;
import response.Response;

public class Main {

    private static final String LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.err.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            List<RecordBatch> batches = new ArrayList<>();
            try (DataInputStream input = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(LOG_PATH)))) {
                while (true) {
                    try {
                        batches.add(RecordBatch.from(input));
                    } catch (EOFException e) {
                        break;
                    }
                }
            }

            Map<String, byte[]> topicIdByName = new HashMap<>();
            Map<String, List<MetadataRecord.PartitionRecord>> partitionsByTopicId = new HashMap<>();
            Set<UUID> existingTopicIds = new HashSet<>();
            Map<UUID, String> nameByTopicId = new HashMap<>();

            for (RecordBatch batch : batches) {
                for (RecordBatch.Record record : batch.records()) {
                    if (record.value() == null) {
                        continue;
                    }

                    try {
                        MetadataRecord meta = MetadataRecord.from(record.value());
                        if (meta instanceof MetadataRecord.TopicRecord t) {
                            topicIdByName.put(t.name(), t.topicId());
                            existingTopicIds.add(toUUID(t.topicId()));
                            nameByTopicId.put(toUUID(t.topicId()), t.name());
                        } else if (meta instanceof MetadataRecord.PartitionRecord p) {
                            String key = hex(p.topicId());
                            partitionsByTopicId.computeIfAbsent(key, k -> new ArrayList<>()).add(p);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("Error parsing record: " + record.value());
                    }
                }
            }

            while (true) {
                clientSocket = serverSocket.accept();
                Socket socket = clientSocket;
                new Thread(() -> {
                    try {
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        while (true) {
                            try {
                                int message_size = in.readInt();
                                short request_api_key = in.readShort();
                                short request_api_version = in.readShort();
                                int correlation_id = in.readInt();

                                switch (request_api_key) {

                                    case 1 -> {
                                        // Request
                                        short clientIdLength = in.readShort();
                                        in.skipBytes(clientIdLength);
                                        in.skipBytes(1);

                                        in.skipBytes(21);
                                        int topicCount = in.readByte() - 1;

                                        List<UUID> requestedTopicIds = new ArrayList<>();
                                        for (int i = 0; i < topicCount; i++) {
                                            byte[] uuidBytes = new byte[16];
                                            in.readFully(uuidBytes);
                                            requestedTopicIds.add(toUUID(uuidBytes));
                                        }

                                        List<FetchResponse.Topic> responses = new ArrayList<>();
                                        for (UUID topicId : requestedTopicIds) {
                                            String topicName = nameByTopicId.get(topicId);
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


                                        // Response
                                        FetchResponse response = FetchResponse.builder()
                                                .correlationId(correlation_id)
                                                .throttleTimeMs(0)
                                                .errorCode((short) 0)
                                                .sessionId(0)
                                                .responses(responses)
                                                .build();
                                        send(out, response);
                                    }
                                    case 18 -> {
                                        in.skipBytes(message_size - 8);

                                        short error_code = 0;
                                        if (request_api_version < 0 || request_api_version > 4) {
                                            error_code = 35;
                                        }

                                        ApiVersionsResponse response = ApiVersionsResponse.builder()
                                                .correlationId(correlation_id)
                                                .errorCode(error_code)
                                                .apiKeys(List.of(
                                                        ApiVersionsResponse.ApiKey.builder()
                                                                .apiKey(18)
                                                                .minVersion(0)
                                                                .maxVersion(4)
                                                                .build(),
                                                        ApiVersionsResponse.ApiKey.builder()
                                                                .apiKey(75)
                                                                .minVersion(0)
                                                                .maxVersion(0)
                                                                .build(),
                                                        ApiVersionsResponse.ApiKey.builder()
                                                                .apiKey(1)
                                                                .minVersion(0)
                                                                .maxVersion(16)
                                                                .build(),
                                                        ApiVersionsResponse.ApiKey.builder()
                                                                .apiKey(0)
                                                                .minVersion(0)
                                                                .maxVersion(11)
                                                                .build()
                                                ))
                                                .throttleTimeMs(0)
                                                .build();

                                        send(out, response);
                                    }

                                    case 75 -> {
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
                                            byte[] topicIdBytes = topicIdByName.get(name);

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
                                                List<PartitionRecord> parts =
                                                        partitionsByTopicId.getOrDefault(hex(topicIdBytes), List.of());
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

                                        DescribeTopicPartitionsResponse response =
                                                DescribeTopicPartitionsResponse.builder()
                                                        .correlationId(correlation_id)
                                                        .throttleTime(0)
                                                        .topics(topics)
                                                        .build();

                                        send(out, response);
                                    }
                                    default -> {
                                        throw new RuntimeException("Unknown api key: " + request_api_version);
                                    }
                                }
                            } catch (EOFException e) {
                                break;
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("IOException: " + e.getMessage());
                    }
                }).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    private static void send(DataOutputStream out, Response response) throws IOException {
        ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bodyStream);
        response.writeTo(body);
        body.flush();

        byte[] byteArray = bodyStream.toByteArray();
        out.writeInt(byteArray.length); // message_size
        out.write(byteArray);
        out.flush();
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
