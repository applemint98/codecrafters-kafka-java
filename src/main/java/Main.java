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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import metadata.MetadataRecord;
import metadata.MetadataRecord.PartitionRecord;
import metadata.RecordBatch;
import response.ApiVersionsResponse;
import response.DescribeTopicPartitionsResponse;
import response.DescribeTopicPartitionsResponse.Partition;
import response.DescribeTopicPartitionsResponse.Topic;
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

            for (RecordBatch batch : batches) {
                for (RecordBatch.Record record : batch.records()) {
                    if (record.value() == null) {
                        continue;
                    }

                    try {
                        MetadataRecord meta = MetadataRecord.from(record.value());
                        if (meta instanceof MetadataRecord.TopicRecord t) {
                            topicIdByName.put(t.name(), t.topicId());
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

                                if (request_api_key == 18) {
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
                                                            .apiKey((short) 18)
                                                            .minVersion((short) 0)
                                                            .maxVersion((short) 4)
                                                            .build(),
                                                    ApiVersionsResponse.ApiKey.builder()
                                                            .apiKey((short) 75)
                                                            .minVersion((short) 0)
                                                            .maxVersion((short) 0)
                                                            .build()
                                            ))
                                            .throttleTimeMs(0)
                                            .build();

                                    send(out, response);
                                } else if (request_api_key == 75) {
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
}
