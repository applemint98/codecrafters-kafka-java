package response;

import static util.ProtocolWriter.writeCompactArray;
import static util.ProtocolWriter.writeCompactString;
import static util.ProtocolWriter.writeEmptyTagBuffer;
import static util.ProtocolWriter.writeIntCompactArray;
import static util.ProtocolWriter.writeUUID;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public record DescribeTopicPartitionsResponse(
        int correlationId,
        int throttleTime,
        List<Topic> topics
) implements Response {

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(correlationId);
        writeEmptyTagBuffer(out);
        out.writeInt(throttleTime);
        writeCompactArray(out, topics, (o, v) -> v.writeTo(o));
        out.writeByte(0xFF);
        writeEmptyTagBuffer(out);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int correlationId;
        private int throttleTime;
        private List<Topic> topics;

        public Builder correlationId(int v) {
            this.correlationId = v;
            return this;
        }

        public Builder throttleTime(int v) {
            this.throttleTime = v;
            return this;
        }

        public Builder topics(List<Topic> v) {
            this.topics = v;
            return this;
        }

        public DescribeTopicPartitionsResponse build() {
            return new DescribeTopicPartitionsResponse(correlationId, throttleTime, topics);
        }
    }

    public record Topic(
            short errorCode,
            String topicName,
            UUID topicId,
            boolean isInternal,
            List<Partition> partitions,
            int topicAuthorizedOperations
    ) {

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeShort(errorCode);
            writeCompactString(out, topicName);
            writeUUID(out, topicId);
            out.writeBoolean(isInternal);
            writeCompactArray(out, partitions, (o, v) -> v.writeTo(o));
            out.writeInt(topicAuthorizedOperations);
            writeEmptyTagBuffer(out);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private short errorCode;
            private String topicName;
            private UUID topicId;
            private boolean isInternal;
            private List<Partition> partitions = List.of();
            private int topicAuthorizedOperations;

            public Builder errorCode(short v) {
                this.errorCode = v;
                return this;
            }

            public Builder topicName(String v) {
                this.topicName = v;
                return this;
            }

            public Builder topicId(UUID v) {
                this.topicId = v;
                return this;
            }

            public Builder isInternal(boolean v) {
                this.isInternal = v;
                return this;
            }

            public Builder partitions(List<Partition> v) {
                this.partitions = v;
                return this;
            }

            public Builder topicAuthorizedOperations(int v) {
                this.topicAuthorizedOperations = v;
                return this;
            }

            public Topic build() {
                return new Topic(errorCode, topicName, topicId, isInternal, partitions, topicAuthorizedOperations);
            }
        }
    }

    public record Partition(
            short errorCode,
            int partitionIndex,
            int leaderId,
            int leaderEpoch,
            List<Integer> replicaNodes,
            List<Integer> isrNodes,
            List<Integer> eligibleLeaderReplicas,
            List<Integer> lastKnownElr,
            List<Integer> offlineReplicas
    ) {

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeShort(errorCode);
            out.writeInt(partitionIndex);
            out.writeInt(leaderId);
            out.writeInt(leaderEpoch);
            writeIntCompactArray(out, replicaNodes);
            writeIntCompactArray(out, isrNodes);
            writeIntCompactArray(out, eligibleLeaderReplicas);
            writeIntCompactArray(out, lastKnownElr);
            writeIntCompactArray(out, offlineReplicas);
            writeEmptyTagBuffer(out);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private short errorCode;
            private int partitionIndex;
            private int leaderId;
            private int leaderEpoch;
            private List<Integer> replicaNodes = List.of();
            private List<Integer> isrNodes = List.of();
            private List<Integer> eligibleLeaderReplicas = List.of();
            private List<Integer> lastKnownElr = List.of();
            private List<Integer> offlineReplicas = List.of();

            public Builder errorCode(short v) {
                this.errorCode = v;
                return this;
            }

            public Builder partitionIndex(int v) {
                this.partitionIndex = v;
                return this;
            }

            public Builder leaderId(int v) {
                this.leaderId = v;
                return this;
            }

            public Builder leaderEpoch(int v) {
                this.leaderEpoch = v;
                return this;
            }

            public Builder replicaNodes(List<Integer> v) {
                this.replicaNodes = v;
                return this;
            }

            public Builder isrNodes(List<Integer> v) {
                this.isrNodes = v;
                return this;
            }

            public Builder eligibleLeaderReplicas(List<Integer> v) {
                this.eligibleLeaderReplicas = v;
                return this;
            }

            public Builder lastKnownElr(List<Integer> v) {
                this.lastKnownElr = v;
                return this;
            }

            public Builder offlineReplicas(List<Integer> v) {
                this.offlineReplicas = v;
                return this;
            }

            public Partition build() {
                return new Partition(errorCode, partitionIndex, leaderId, leaderEpoch, replicaNodes, isrNodes,
                        eligibleLeaderReplicas, lastKnownElr, offlineReplicas);
            }
        }
    }
}
