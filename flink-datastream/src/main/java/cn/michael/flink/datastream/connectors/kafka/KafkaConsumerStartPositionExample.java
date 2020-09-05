package cn.michael.flink.datastream.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Created by hufenggang on 2020/9/5.
 */
public class KafkaConsumerStartPositionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();;

        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "127.0.0.1:9092");
        pros.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), pros);

        consumer.setStartFromEarliest(); // 最早记录开始消费
        consumer.setStartFromLatest(); // 最后记录开始消费
        consumer.setStartFromTimestamp(1000); // 从指定的时间戳开始（毫秒为单位）
        consumer.setStartFromGroupOffsets(); // 默认配置

        DataStream<String> stream = env.addSource(consumer);

        env.execute("KafkaConsumerStartPositionExample");
    }
}
