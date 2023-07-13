package com.yh.flink.api;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class socketStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 流批一体：默认是Stream
        //env.setRuntimeMode()

        // 使用createLocalEnvironmentWithWebUI方法时，IDEA运行时也可以看到webui，一般用于本地测试
        // 需要引入一个依赖 flink-runtime
        // 在idea运行，不指定并行度，默认时电脑的线程数
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 7777);

        dataStreamSource.flatMap(
                (String value, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] split = value.split(" ");
                    for (String word : split) {
                        collector.collect(Tuple2.of(word,1));
                    }
                }).setParallelism(2)
                // Lambda表达式中，Java泛型擦出的存在，特殊处理
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute();





    }
}
