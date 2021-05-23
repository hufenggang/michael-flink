package cn.michael.flink.datastream.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by hufenggang on 2020/12/27.
 */
public class WindowsExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Tuple2<Long, Long>> stream = env.addSource(new DataSource());

        stream
            .keyBy(value -> value.f0)
            .window(SlidingProcessingTimeWindows.of(Time.milliseconds(2500), Time.milliseconds(500)))
            .reduce(new SummingReducer())
            .addSink(new SinkFunction<Tuple2<Long, Long>>() {
                @Override
                public void invoke(Tuple2<Long, Long> value) throws Exception {

                }
            });

        env.execute("WindowsExample");


    }

    public static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }


    private static class DataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
            final long startTime = System.currentTimeMillis();

            final long numElements = 20000000;
            final long numKeys = 10000;
            long val = 1L;
            long count = 0L;

            while (running && count < numElements) {
                count++;
                ctx.collect(new Tuple2<>(val++, 1L));

                if (val > numKeys) {
                    val = 1L;
                }
            }

            final long endTime = System.currentTimeMillis();
            System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
