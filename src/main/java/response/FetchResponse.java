package response;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import util.ProtocolWriter;

public record FetchResponse(
        int correlationId,
        int throttleTimeMs,
        short errorCode,
        int sessionId,
        List<Response> responses
) implements Response {

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(correlationId);
        out.writeInt(throttleTimeMs);
        out.writeShort(errorCode);
        out.writeInt(sessionId);
        ProtocolWriter.writeCompactArray(out, responses, (o, v) -> v.writeTo(o));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int correlationId;
        private int throttleTimeMs;
        private short errorCode;
        private int sessionId;
        private List<Response> responses;

        public Builder correlationId(int correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder throttleTimeMs(int throttleTimeMs) {
            this.throttleTimeMs = throttleTimeMs;
            return this;
        }

        public Builder errorCode(short errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public Builder sessionId(int sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public Builder responses(List<Response> responses) {
            this.responses = responses;
            return this;
        }

        public FetchResponse build() {
            return new FetchResponse(correlationId, throttleTimeMs, errorCode, sessionId, responses);
        }
    }

    public record Response(

    ) {
        public void writeTo(DataOutputStream out) throws IOException {
            out.write(0);
        }
    }
}
