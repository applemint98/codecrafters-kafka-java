import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

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
            clientSocket = serverSocket.accept();

            DataInputStream in = new DataInputStream(clientSocket.getInputStream());
            int message_size = in.readInt();
            short request_api_key = in.readShort();
            short request_api_version = in.readShort();
            int correlation_id = in.readInt();

            short error_code = 0;
            if (request_api_version < 0 || request_api_version > 4) {
                error_code = 35;
            }
            DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
            out.writeInt(0);
            out.writeInt(correlation_id);
            out.writeShort(error_code);
            out.flush();
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
