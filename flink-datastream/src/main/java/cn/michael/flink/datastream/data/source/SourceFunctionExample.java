package cn.michael.flink.datastream.data.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by hufenggang on 2020/8/30.
 *
 * 自定义数据源-SourceFunction接口实现方式
 */
public class SourceFunctionExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFunction<Long>() {

            private long count = 0;
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                while (isRunning && count < 1000) {
                    ctx.collect(count);
                    count++;
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).print();

        env.execute();
    }
}
