package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonToParquetConverter {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing every 1 second to ensure data is flushed
        env.enableCheckpointing(1000); // 1 second

        String[] jsonData = {
                "{\"id\": 1, \"name\": \"John Doe\", \"age\": 25}",
                "{\"id\": 2, \"name\": \"Jane Smith\", \"age\": 30}",
                "{\"id\": 3, \"name\": \"Bob Johnson\", \"age\": 35}"
        };

        DataStream<String> inputStream = env.fromElements(jsonData);
        DataStream<UserRecord> recordStream = inputStream.process(new JsonToRecordConverter());

        // Configure the StreamingFileSink for bulk format
        StreamingFileSink<UserRecord> sink = StreamingFileSink
                .forBulkFormat(
                        new Path("output/parquet-output"),
                        ParquetAvroWriters.forReflectRecord(UserRecord.class)
                )
                .withBucketAssigner(new BasePathBucketAssigner<>()) // Avoid date-based subdirectories
                .build();

        recordStream.addSink(sink);

        // Execute and wait briefly to ensure data is flushed
        env.execute("JSON to Parquet Conversion");
        Thread.sleep(2000); // Wait 2 seconds to allow checkpointing to complete
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
            JsonNode jsonNode = mapper.readTree(json);
            UserRecord record = new UserRecord(
                    jsonNode.get("id").asInt(),
                    jsonNode.get("name").asText(),
                    jsonNode.get("age").asInt()
            );
            out.collect(record);
        }
    }
}