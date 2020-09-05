package cn.michael.flink.datastream.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by hufenggang on 2020/9/5.
 */
public class KafkaConsumerStartPositionExample02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();;

        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "127.0.0.1:9092");
        pros.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), pros);

        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("my-topic", 0), 23L);
        specificStartOffsets.put(new KafkaTopicPartition("my-topic", 1), 24L);
        specificStartOffsets.put(new KafkaTopicPartition("my-topic", 2), 25L);

        consumer.setStartFromSpecificOffsets(specificStartOffsets);

        DataStream<String> stream = env.addSource(consumer);

        env.execute("KafkaConsumerStartPositionExample02");
    }
}
