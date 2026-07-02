import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import metadata.MetadataRecord;
import metadata.RecordBatch;

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
            Map<String, List<RecordBatch.Record>> ignore;
            Map<String, List<MetadataRecord.PartitionRecord>> partitionsByTopicId = new HashMap<>();

            for  (RecordBatch batch : batches) {
                for (RecordBatch.Record record : batch.records()) {
                    if (record.value() == null) continue;

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

                                    ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
                                    DataOutputStream body = new DataOutputStream(bodyStream);

                                    body.writeInt(correlation_id);
                                    body.writeShort(error_code);
                                    body.writeByte(3); // api_keys array, real count + 1 because of null

                                    // ApiVersions
                                    body.writeShort(18);
                                    body.writeShort(0);
                                    body.writeShort(4);
                                    body.writeByte(0);

                                    // DescribeTopicPartitions
                                    body.writeShort(75);
                                    body.writeShort(0);
                                    body.writeShort(0);
                                    body.writeByte(0);

                                    // TagBuffer & throttle_time_ms
                                    body.writeInt(0);
                                    body.writeByte(0);
                                    body.flush();

                                    byte[] byteArray = bodyStream.toByteArray();
                                    out.writeInt(byteArray.length); // message_size
                                    out.write(byteArray);
                                    out.flush();
                                } else if (request_api_key == 75) {
                                    // Request
                                    short client_id_length = in.readShort();
                                    in.skipBytes(client_id_length);
                                    in.skipBytes(1); // TagBuffer

                                    List<String> topicNameStrings = new ArrayList<>();

                                    List<byte[]> topicNames = new ArrayList<>();
                                    byte topicArrayLength = (byte) (in.readByte() - 1);
                                    for  (int i = 0; i < topicArrayLength; i++) {
                                        byte topicNameLength = (byte) (in.readByte() - 1);
                                        byte[] topicNameBytes = new byte[topicNameLength];
                                        in.readFully(topicNameBytes);
                                        topicNames.add(topicNameBytes);
                                        topicNameStrings.add(new String(topicNameBytes, StandardCharsets.UTF_8));
                                        // TODO: TagBuffer skip or read which one is best?
                                        in.skipBytes(1); // TagBuffer
                                    }

                                    int responsePartitionLimit = in.readInt();
                                    byte cursor = in.readByte();
                                    in.skipBytes(1); // TagBuffer

                                    // Response
                                    ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
                                    DataOutputStream body = new DataOutputStream(bodyStream);

                                    body.writeInt(correlation_id);
                                    body.writeByte(0);
                                    body.writeInt(0); // throttle time
                                    for (String name : topicNameStrings) {
                                        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
                                        byte[] topicId = topicIdByName.get(name);   // 맵 조회

                                        if (topicId == null) {
                                            body.writeShort(3);
                                            body.writeByte(nameBytes.length + 1);
                                            body.write(nameBytes);
                                            body.write(new byte[16]);   // writeInt(0)×4 to one line
                                            body.writeByte(0);          // is_internal
                                            body.writeByte(1);          // partitions
                                            body.writeInt(0);           // authorized_ops
                                            body.writeByte(0);          // TAG
                                        } else {
                                            List<MetadataRecord.PartitionRecord> parts =
                                                    partitionsByTopicId.getOrDefault(hex(topicId), List.of());

                                            body.writeShort(0);         // error_code 0
                                            body.writeByte(nameBytes.length + 1);
                                            body.write(nameBytes);
                                            body.write(topicId);        // UUID
                                            body.writeByte(0);          // is_internal
                                            body.writeByte(parts.size() + 1);

                                            for (MetadataRecord.PartitionRecord p : parts) {
                                                body.writeShort(0);              // partition error_code
                                                body.writeInt(p.partitionId());
                                                body.writeInt(p.leaderId());
                                                body.writeInt(p.leaderEpoch());
                                                writeIntArray(body, p.replicaNodes());
                                                writeIntArray(body, p.isrNodes());
                                                body.writeByte(1);               // eligible_leader_replicas
                                                body.writeByte(1);               // last_known_elr
                                                body.writeByte(1);               // offline_replicas
                                                body.writeByte(0);               // partition TAG
                                            }
                                            body.writeInt(0);           // authorized_ops
                                            body.writeByte(0);          // TAG
                                        }
                                    }
                                    body.writeByte(-1); // next_cursor
                                    body.writeByte(0);  // TAG

                                    byte[] byteArray = bodyStream.toByteArray();
                                    out.writeInt(byteArray.length); // message_size
                                    out.write(byteArray);
                                    out.flush();
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

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private static void writeIntArray(DataOutputStream body, List<Integer> arr) throws IOException {
        body.writeByte(arr.size() + 1);
        for (int v : arr) body.writeInt(v);
    }
}
