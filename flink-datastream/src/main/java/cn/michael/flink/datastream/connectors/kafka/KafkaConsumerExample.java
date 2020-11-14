package cn.michael.flink.datastream.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Created by hufenggang on 2020/9/5.
 */
public class KafkaConsumerExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();;

        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "127.0.0.1:9092");
        pros.setProperty("group.id", "test");
        DataStream<String> stream =env.addSource(new FlinkKafkaConsumer<>("topic-test", new SimpleStringSchema(), pros));
        stream.print();
        env.execute("KafkaConsumerExample");
    }
}
