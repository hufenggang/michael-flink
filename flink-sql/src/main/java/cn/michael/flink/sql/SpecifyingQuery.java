package cn.michael.flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created by hufenggang on 2020/11/8.
 * <p>
 * 指定查询
 */
public class SpecifyingQuery {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "212.129.246.160:9092");
        pros.setProperty("group.id", "test");
        DataStreamSource<String> ds = env.addSource(new FlinkKafkaConsumer<>("topic-test", new SimpleStringSchema(), pros));

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> ds2 = ds.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {

            @Override
            public Tuple3<Long, String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple3<>(Long.parseLong(split[0]), split[1], Integer.parseInt(split[2]));
            }
        });

        // SQL query with an inlined (unregistered) table
        Table table = tableEnv.fromDataStream(ds, $("user"), $("product"), $("amount"));
        Table result = tableEnv.sqlQuery(
            "SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%Rubber%'");

        // SQL query with a registered table
        // register the DataStream as view "Orders"
        tableEnv.createTemporaryView("Orders", ds, $("user"), $("product"), $("amount"));
        // run a SQL query on the Table and retrieve the result as a new Table
        Table result2 = tableEnv.sqlQuery(
            "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

        // create and register a TableSink
        final Schema schema = new Schema()
            .field("product", DataTypes.STRING())
            .field("amount", DataTypes.INT());

//        tableEnv.connect(new FileSystem().path("/path/to/file"))
//            .withFormat(...)
//        .withSchema(schema)
//            .createTemporaryTable("RubberOrders");

        // run an INSERT SQL on the Table and emit the result to the TableSink
        tableEnv.executeSql(
            "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");


    }
}
