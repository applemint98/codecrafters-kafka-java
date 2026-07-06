import handler.ApiHandler;
import handler.ApiVersionsHandler;
import handler.DescribeTopicPartitionsHandler;
import handler.FetchHandler;
import handler.ProduceHandler;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import metadata.MetadataStore;
import response.Response;
import server.RequestHeader;

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

            Map<Short, ApiHandler> handlers = Map.of(
                    (short) 0, new ProduceHandler(store),
                    (short) 1, new FetchHandler(store),
                    (short) 18, new ApiVersionsHandler(),
                    (short) 75, new DescribeTopicPartitionsHandler(store)
            );

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
                                ApiHandler handler = handlers.get(header.apiKey());
                                if (handler == null) {
                                    throw new RuntimeException("Unknown api key: " + header.apiKey());
                                }
                                send(out, handler.handle(header, in));
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
}
