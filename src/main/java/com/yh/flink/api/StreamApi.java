package com.yh.flink.api;

import com.yh.entity.MyData;
import com.yh.utils.FakerUil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class StreamApi {

    public static void main(String[] args) throws Exception {
        // 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // map操作
        SingleOutputStreamOperator<MyData> source = env.addSource(new SourceData()).map(new MapFunction<MyData, MyData>() {
            @Override
            public MyData map(MyData myData) throws Exception {
                // 转化为日期格式
                String format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(myData.getTimestamp()));
                return new MyData(myData.getKeyId(), myData.getTimestamp(), format, myData.getValue());
            }
        });


        // 输出到 MySQL
        SinkFunction<MyData> sinkFunction = JdbcSink.sink("insert into MyData values(?,?,?,?)"
                , new JdbcStatementBuilder<MyData>() {
                    @Override
                    public void accept(PreparedStatement ps, MyData myData) throws SQLException {

                        ps.setInt(1, myData.getKeyId());
                        ps.setLong(2, myData.getTimestamp());
                        ps.setString(3,myData.getEtime());
                        ps.setDouble(4, myData.getValue());
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

        // sink
        source.addSink(sinkFunction);

        // 打印sink结果
        source.print();

        env.execute();
    }




    /**
     * 自定义随机生成模拟数据：
     *  返回类型为 MyData的POJO类
     */
    public static class SourceData implements SourceFunction<MyData> {
        // 定义标志位，用来控制数据的产生
        private boolean isRunning = true;
        private final Random random = new Random(0);

        @Override
        public void run(SourceContext<MyData> sourceContext) throws Exception {
            while (isRunning) {
                // 随机生成一个时间戳
                Long aLong = Long.valueOf(FakerUil.randomTime("2022-01-01 00:00:00", "2022-04-01 00:00:00"));

                MyData myData = new MyData();
                myData.setKeyId(FakerUil.randomNum(1,150));
                myData.setTimestamp(aLong);
                myData.setEtime(String.valueOf(aLong));
                myData.setValue(FakerUil.randomNum(100,1500));

                sourceContext.collect(myData);
                Thread.sleep(500L);
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
