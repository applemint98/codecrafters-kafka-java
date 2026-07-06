package handler;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import response.ApiVersionsResponse;
import response.Response;
import server.RequestHeader;

public class ApiVersionsHandler implements ApiHandler {

    @Override
    public Response handle(RequestHeader header, DataInputStream in) throws IOException {
        in.skipBytes(header.messageSize() - 8); // temp: body 버리기

        short error_code = 0;
        if (header.apiVersion() < 0 || header.apiVersion() > 4) {
            error_code = 35;
        }

        return ApiVersionsResponse.builder()
                .correlationId(header.correlationId())
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
    }
}
