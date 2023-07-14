package com.yh.flink.api;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class UnionStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> source2 = env.fromElements(6, 7, 8, 9, 10);
        DataStreamSource<String> source3 = env.fromElements("a","b","c","d","e");

        // 合并流
        //DataStream<Integer> unionResult = source1.union(source2);

        // connect
        ConnectedStreams<Integer, String> connect = source1.connect(source3);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "source1=" + value;
            }
            @Override
            public String map2(String value) throws Exception {
                return "source2=" + value;
            }
        });

        map.print();

        env.execute();
    }
}
