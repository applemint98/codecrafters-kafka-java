import handler.ApiVersionsHandler;
import handler.DescribeTopicPartitionsHandler;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import metadata.MetadataRecord.PartitionRecord;
import metadata.MetadataStore;
import response.FetchResponse;
import response.ProduceResponse;
import response.Response;
import server.RequestHeader;
import util.Decoder;

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

            MetadataStore store = MetadataStore.load(LOG_PATH);

            while (true) {
                clientSocket = serverSocket.accept();
                Socket socket = clientSocket;
                new Thread(() -> {
                    try {
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        while (true) {
                            try {
                                RequestHeader header = RequestHeader.from(in);

                                switch (header.apiKey()) {

                                    case 0 -> {
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

                                        ProduceResponse response = ProduceResponse.builder()
                                                .correlationId(header.correlationId())
                                                .topics(topics)
                                                .throttleTime(0)
                                                .build();

                                        send(out, response);
                                    }

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

                                        // Response
                                        FetchResponse response = FetchResponse.builder()
                                                .correlationId(header.correlationId())
                                                .throttleTimeMs(0)
                                                .errorCode((short) 0)
                                                .sessionId(0)
                                                .responses(responses)
                                                .build();
                                        send(out, response);
                                    }
                                    case 18 -> {
                                        Response response = new ApiVersionsHandler().handle(header, in);
                                        send(out, response);
                                    }
                                    case 75 -> {
                                        Response response = new DescribeTopicPartitionsHandler(store).handle(header, in);
                                        send(out, response);
                                    }
                                    default -> {
                                        throw new RuntimeException("Unknown api key: " + header.apiVersion());
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
        out.writeInt(byteArray.length);
        out.write(byteArray);
        out.flush();
    }

    private static UUID toUUID(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low);
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
