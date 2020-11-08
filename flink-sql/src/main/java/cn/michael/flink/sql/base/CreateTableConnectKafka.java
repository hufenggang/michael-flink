package cn.michael.flink.sql.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * Created by hufenggang on 2020/11/8.
 * <p>
 * 消费Kafka数据
 */
public class CreateTableConnectKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        tableEnv.connect(new Kafka()
            .version("0.11") // 定义Kafka版本
            .topic("") // 定义主题
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092"))
            .withFormat(new OldCsv()) // 定义从外部文件读取数据之后的格式化方法
            .withSchema(new Schema() // 定义表结构
                .field("id", DataTypes.STRING())
                .field("name", DataTypes.STRING()))
            .createTemporaryTable("v_data"); // 在表环境里注册一张表


        // 测试输出
        Table inputTable = tableEnv.from("v_data");
        inputTable.printSchema();

        bsEnv.execute("CreateTableConnectorTables");
    }
}
