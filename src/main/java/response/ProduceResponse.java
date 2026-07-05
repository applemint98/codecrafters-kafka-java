package response;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import util.ProtocolWriter;

public record ProduceResponse(
        int correlationId,
        List<Topic> topics,
        int throttleTime
) implements Response {


    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(correlationId);
        ProtocolWriter.writeEmptyTagBuffer(out);
        ProtocolWriter.writeCompactArray(out, topics, (o, v) -> v.writeTo(o));
        out.writeInt(throttleTime);
        ProtocolWriter.writeEmptyTagBuffer(out);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int correlationId;
        private List<Topic> topics = List.of();
        private int throttleTime;

        public Builder correlationId(int v) {
            this.correlationId = v;
            return this;
        }

        public Builder topics(List<Topic> v) {
            this.topics = v;
            return this;
        }

        public Builder throttleTime(int v) {
            this.throttleTime = v;
            return this;
        }

        public ProduceResponse build() {
            return new ProduceResponse(correlationId, topics, throttleTime);
        }
    }

    public record Topic(
            String name,
            List<Partition> partitions
    ) {

        public void writeTo(DataOutputStream out) throws IOException {
            ProtocolWriter.writeCompactString(out, name);
            ProtocolWriter.writeCompactArray(out, partitions, (o, v) -> v.writeTo(o));
            ProtocolWriter.writeEmptyTagBuffer(out);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String name;
            private List<Partition> partitions;

            public Builder name(String v) {
                this.name = v;
                return this;
            }

            public Builder partitions(List<Partition> v) {
                this.partitions = v;
                return this;
            }

            public Topic build() {
                return new Topic(name, partitions);
            }
        }
    }

    public record Partition(
            int index,
            short errorCode,
            long baseOffset,
            long logAppendTimeMs,
            long logStartOffset
    ) {
        public void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(index);
            out.writeShort(errorCode);
            out.writeLong(baseOffset);
            out.writeLong(logAppendTimeMs);
            out.writeLong(logStartOffset);
            ProtocolWriter.writeCompactArray(out, List.of(), (o, v) -> {});
            out.writeByte(0);
            ProtocolWriter.writeEmptyTagBuffer(out);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int index;
            private short errorCode;
            private long baseOffset;
            private long logAppendTimeMs;
            private long logStartOffset;

            public Builder index(int v) { this.index = v; return this; }
            public Builder errorCode(short v) { this.errorCode = v; return this; }
            public Builder baseOffset(long v) { this.baseOffset = v; return this; }
            public Builder logAppendTimeMs(long v) { this.logAppendTimeMs = v; return this; }
            public Builder logStartOffset(long v) { this.logStartOffset = v; return this; }

            public Partition build() {
                return new Partition(index, errorCode, baseOffset, logAppendTimeMs, logStartOffset);
            }
        }
    }
}
