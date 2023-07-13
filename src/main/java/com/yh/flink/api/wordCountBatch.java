package com.yh.flink.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class wordCountBatch {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据：通过文件读取
        DataStreamSource<String> dataStreamSource = env.readTextFile("/Users/liangyuhang/Documents/ideaProj/ElaticsearchAPI/bigdata-api/bigdata-api/src/main/resources/words.txt");

        // 对数据进行切分、转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 按照 空格 进行切分单词
                String[] split = value.split(" ");
                for (String word : split) {
                    //  转换为（word,1）的格式
                    Tuple2<String, Integer> wordTuple = Tuple2.of(word, 1);
                    // 使用Collector向下游发送数据
                    collector.collect(wordTuple);
                }
            }
        });

        // 按照word分组
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream =
            tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                @Override
                public String getKey(Tuple2<String, Integer> stringStringTuple2) throws Exception {
                    return stringStringTuple2.f0;
                }
        });

        // 各分组内聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult = tuple2StringKeyedStream.sum(1);

        // 输出
        sumResult.print();

        env.execute();
    }
}
