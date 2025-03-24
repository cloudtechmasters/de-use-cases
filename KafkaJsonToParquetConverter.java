package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaJsonToParquetConverter {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(KafkaJsonToParquetConverter.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing every 1 second with exactly-once semantics
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // Set checkpoint storage directory for durability
        env.getCheckpointConfig().setCheckpointStorage("file:///C:/Users/muppa/FlexDay/pr-workspace/flink-json-to-parquet/checkpoints");
        // Retain checkpoints on cancellation for manual restarts
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Kafka source configuration
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092") // Adjust to your Kafka broker
                .setTopics("test-topic")               // Kafka topic name
                .setGroupId("flink-consumer-group")    // Consumer group ID
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetsInitializer.earliest().getAutoOffsetResetStrategy())) // Resume from committed offsets, fallback to earliest
                .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserializer for string messages
                .build();

        // Create DataStream from Kafka
        DataStream<String> inputStream = env.fromSource(kafkaSource,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // Process JSON into UserRecord
        DataStream<UserRecord> recordStream = inputStream.process(new JsonToRecordConverter());

        // Configure the StreamingFileSink for bulk format
        StreamingFileSink<UserRecord> sink = StreamingFileSink
                .forBulkFormat(
                        new Path("output/parquet-output-kafka"),
                        ParquetAvroWriters.forReflectRecord(UserRecord.class)
                )
                .withBucketAssigner(new BasePathBucketAssigner<>()) // Avoid date-based subdirectories
                .build();

        recordStream.addSink(sink);

        // Execute the Flink job with logging
        logger.info("Starting Kafka JSON to Parquet Conversion job...");
        env.execute("Kafka JSON to Parquet Conversion");
    }

    // POJO class to represent the data
    public static class UserRecord {
        private int id;
        private String name;
        private int age;

        // Default constructor required for Flink/Parquet
        public UserRecord() {}

        public UserRecord(int id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        // Getters and setters required for Flink/Parquet reflection
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
    }

    public static class JsonToRecordConverter extends ProcessFunction<String, UserRecord> {
        @Override
        public void processElement(String json, Context ctx, Collector<UserRecord> out) throws Exception {
            try {
                logger.debug("Processing message: {}", json);
                JsonNode jsonNode = mapper.readTree(json);

                // Check for null fields
                JsonNode idNode = jsonNode.get("id");
                JsonNode nameNode = jsonNode.get("name");
                JsonNode ageNode = jsonNode.get("age");

                if (idNode == null || nameNode == null || ageNode == null) {
                    logger.warn("Skipping invalid JSON message: missing field(s) in {}", json);
                    return; // Skip this record
                }

                // Convert with defaults if necessary
                int id = idNode.isInt() ? idNode.asInt() : -1; // Default to -1 if not an int
                String name = nameNode.isTextual() ? nameNode.asText() : "Unknown"; // Default to "Unknown"
                int age = ageNode.isInt() ? ageNode.asInt() : -1; // Default to -1 if not an int

                UserRecord record = new UserRecord(id, name, age);
                out.collect(record);
            } catch (Exception e) {
                logger.error("Failed to process JSON message: {}", json, e);
                // Skip invalid messages to keep the job running
            }
        }
    }
}