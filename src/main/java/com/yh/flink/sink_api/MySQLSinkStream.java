package com.yh.flink.sink_api;

import com.yh.entity.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLSinkStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 必须开启checkpoint,否则 精准一次 无法写入Kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                });

        /**
         *
         */
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink("insert into water_sensor values(?,?,?)"
                , new JdbcStatementBuilder<WaterSensor>() {  // 写成 lamdba 表达式 option+enter
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/bigData?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("00000000")
                        // 检查连接超时
                        .withConnectionCheckTimeoutSeconds(60)
                        .build());

        source.addSink(jdbcSink);

        env.execute();
    }
}
