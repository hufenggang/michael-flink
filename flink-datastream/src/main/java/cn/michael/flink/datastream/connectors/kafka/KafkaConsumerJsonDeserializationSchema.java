package cn.michael.flink.datastream.connectors.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

/**
 * Created by hufenggang on 2020/11/14.
 *
 * JsonDeserializationSchema
 */
public class KafkaConsumerJsonDeserializationSchema {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "127.0.0.1:9092");
        pros.setProperty("group.id", "test");

        DataStreamSource<ObjectNode> stream =env.addSource(new FlinkKafkaConsumer<>("topic-test", new JSONKeyValueDeserializationSchema(true), pros));

        stream.map(new MapFunction<ObjectNode, Object>() {
            @Override
            public Object map(ObjectNode jsonNodes) throws Exception {
                return jsonNodes.get("value").get("name").toString();
            }
        }).print();

        env.execute("KafkaConsumerJsonDeserializationSchema");
    }
}
