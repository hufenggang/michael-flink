package cn.michael.flink.app.service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Created by hufenggang on 2021/1/23.
 */
public class TopNCalculateService {

    public static void run(FlinkKafkaConsumer<String> consumer) {

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

    }
}
