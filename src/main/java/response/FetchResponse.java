package response;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import util.ProtocolWriter;

public record FetchResponse(
        int correlationId,
        int throttleTimeMs,
        short errorCode,
        int sessionId,
        List<Topic> responses
) implements Response {

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(correlationId);
        ProtocolWriter.writeEmptyTagBuffer(out);
        out.writeInt(throttleTimeMs);
        out.writeShort(errorCode);
        out.writeInt(sessionId);
        ProtocolWriter.writeCompactArray(out, responses, (o, v) -> v.writeTo(o));
        ProtocolWriter.writeEmptyTagBuffer(out);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int correlationId;
        private int throttleTimeMs;
        private short errorCode;
        private int sessionId;
        private List<Topic> responses;

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

        public Builder responses(List<Topic> responses) {
            this.responses = responses;
            return this;
        }

        public FetchResponse build() {
            return new FetchResponse(correlationId, throttleTimeMs, errorCode, sessionId, responses);
        }
    }

    public record Topic(
            UUID topicId,
            List<Partition> partitions
    ) {
        public void writeTo(DataOutputStream out) throws IOException {
            ProtocolWriter.writeUUID(out, topicId);
            ProtocolWriter.writeCompactArray(out, partitions, (o, v) -> v.writeTo(o));
            ProtocolWriter.writeEmptyTagBuffer(out);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private UUID topicId;
            private List<Partition> partitions;

            public Builder topicId(UUID v) {
                this.topicId = v;
                return this;
            }

            public Builder partitions(List<Partition> v) {
                this.partitions = v;
                return this;
            }

            public Topic build() {
                return new Topic(topicId, partitions);
            }
        }
    }

    public record Partition(
            int partitionIndex,
            short errorCode,
            long highWatermark,
            long lastStableOffset,
            long logStartOffset,
            List<?> abortedTransactions,
            int preferredReadReplica,
            byte[] records
    ) {
        public void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(partitionIndex);
            out.writeShort(errorCode);
            out.writeLong(highWatermark);
            out.writeLong(lastStableOffset);
            out.writeLong(logStartOffset);
            ProtocolWriter.writeCompactArray(out, abortedTransactions, (o, v) -> {
            });
            out.writeInt(preferredReadReplica);
            ProtocolWriter.writeCompactBytes(out, records);
            ProtocolWriter.writeEmptyTagBuffer(out);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int partitionIndex;
            private short errorCode;
            private long highWatermark;
            private long lastStableOffset;
            private long logStartOffset;
            private List<?> abortedTransactions;
            private int preferredReadReplica;
            private byte[] records;

            public Builder partitionIndex(int v) {
                this.partitionIndex = v;
                return this;
            }

            public Builder errorCode(short v) {
                this.errorCode = v;
                return this;
            }

            public Builder highWatermark(long v) {
                this.highWatermark = v;
                return this;
            }

            public Builder lastStableOffset(long v) {
                this.lastStableOffset = v;
                return this;
            }

            public Builder logStartOffset(long v) {
                this.logStartOffset = v;
                return this;
            }

            public Builder abortedTransactions(List<?> v) {
                this.abortedTransactions = v;
                return this;
            }

            public Builder preferredReadReplica(int v) {
                this.preferredReadReplica = v;
                return this;
            }

            public Builder records(byte[] v) {
                this.records  = v;
                return this;
            }

            public Partition build() {
                return new Partition(partitionIndex, errorCode, highWatermark, lastStableOffset, logStartOffset,
                        abortedTransactions, preferredReadReplica, records);
            }
        }


    }
}
