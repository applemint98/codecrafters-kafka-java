import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.err.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.

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

                                    List<byte[]> topicNames = new ArrayList<>();
                                    byte topicArrayLength = (byte) (in.readByte() - 1);
                                    for  (int i = 0; i < topicArrayLength; i++) {
                                        byte topicNameLength = (byte) (in.readByte() - 1);
                                        byte[] topicNameBytes = new byte[topicNameLength];
                                        in.readFully(topicNameBytes);
                                        topicNames.add(topicNameBytes);
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
                                    body.writeByte(topicArrayLength + 1); // arrayLength
                                    for (byte[] topicName : topicNames) {
                                        body.writeShort(3); // error_code
                                        body.writeByte(topicName.length + 1);
                                        body.write(topicName);
                                        body.writeInt(0); // topic id all zeros
                                        body.writeInt(0);
                                        body.writeInt(0);
                                        body.writeInt(0);
                                        body.writeByte(0); // is_internal: false
                                        body.writeByte(1); // 0 elements
                                        body.writeInt(0);
                                        body.writeByte(0); // TagBuffer
                                    }
                                    body.writeByte(-1); // next_cursor
                                    body.writeByte(0); // TagBuffer

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
}
