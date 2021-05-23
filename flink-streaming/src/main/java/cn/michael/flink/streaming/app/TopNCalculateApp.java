package cn.michael.flink.streaming.app;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;

/**
 * Created by hufenggang on 2021/1/10.
 *
 * {"locTime":"2020-12-28 12:32:23","courierId":12,"other":"aaa"}
 */
public class TopNCalculateApp {
    private static final String topics = "";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "127.0.0.1:9092");
        pros.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            topics,
            new SimpleStringSchema(),
            pros);



        consumer.assignTimestampsAndWatermarks(WatermarkStrategy
            .<String>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                @Override
                public long extractTimestamp(String element, long recordTimestamp) {
                    String locTime = "";
                    try {
                        Gson gson = new Gson();
                        Map<String, String> map = gson.fromJson(element, new TypeToken<Map<String, String>>() {
                        }.getType());
                        locTime = map.get("locTime");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    LocalDateTime startDateTime = LocalDateTime.parse(locTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    long milli = startDateTime.toInstant(OffsetTime.now().getOffset()).toEpochMilli();
                    return milli;
                }
            }).withIdleness(Duration.ofSeconds(1)));

        env.addSource(consumer).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return true;
            }
        }).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                Gson gson = new Gson();
                Map<String, String> map = gson.fromJson(value, new TypeToken<Map<String, String>>() {
                }.getType());
                String courierId = map.get("courierId");
                String day = map.get("locTime").split(" ")[0].replace("-", "");
                return day + "-" + courierId;
            }
        }).window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
            .allowedLateness(Time.minutes(1))
            .trigger(ContinuousEventTimeTrigger.of(Time.seconds(30)))
            .evictor(TimeEvictor.of(Time.seconds(0), true))
            .process(new ProcessWindowFunction<String, Object, String, TimeWindow>() {
                @Override
                public void process(String s, Context context, Iterable<String> elements, Collector<Object> out) throws Exception {

                }
            });


        env.execute("") ;
    }
}
