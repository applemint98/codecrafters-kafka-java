package response;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import util.ProtocolWriter;

public record ApiVersionsResponse(
        int correlationId,
        short errorCode,
        List<ApiKey> apiKeys,
        int throttleTimeMs
) {
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(correlationId);
        out.writeShort(errorCode);
        ProtocolWriter.writeCompactArray(out, apiKeys, (o, v) -> v.writeTo(o));
        out.writeInt(throttleTimeMs);
        ProtocolWriter.writeEmptyTagBuffer(out);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int correlationId;
        private short errorCode;
        private List<ApiKey> apiKeys = List.of();
        private int throttleTimeMs;

        public Builder correlationId(int v) {
            this.correlationId = v;
            return this;
        }

        public Builder errorCode(short v) {
            this.errorCode = v;
            return this;
        }

        public Builder apiKeys(List<ApiKey> v) {
            this.apiKeys = v;
            return this;
        }

        public Builder throttleTimeMs(int v) {
            this.throttleTimeMs = v;
            return this;
        }

        public ApiVersionsResponse build() {
            return new ApiVersionsResponse(correlationId, errorCode, apiKeys, throttleTimeMs);
        }
    }

    public record ApiKey(
            short apiKey,
            short minVersion,
            short maxVersion
    ) {
        public void writeTo(DataOutputStream out) throws IOException {
            out.writeShort(apiKey);
            out.writeShort(minVersion);
            out.writeShort(maxVersion);
            ProtocolWriter.writeEmptyTagBuffer(out);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private short apiKey;
            private short minVersion;
            private short maxVersion;

            public Builder apiKey(short v) {
                this.apiKey = v;
                return this;
            }

            public Builder minVersion(short v) {
                this.minVersion = v;
                return this;
            }

            public Builder maxVersion(short v) {
                this.maxVersion = v;
                return this;
            }

            public ApiKey build() {
                return new ApiKey(apiKey, minVersion, maxVersion);
            }
        }
    }
}
