package handler;

import java.io.DataInputStream;
import java.io.IOException;
import response.Response;
import server.RequestHeader;

public interface ApiHandler {

    Response handle(RequestHeader header, DataInputStream in) throws IOException;
}
