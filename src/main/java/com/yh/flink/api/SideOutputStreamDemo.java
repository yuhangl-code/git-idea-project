package com.yh.flink.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  实现connect，实现两个流之间的inner join连接（互相匹配）
 *  数据源为fromElement自己造数，实现简单逻辑
 */
public class SideOutputStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );

        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 1),
                Tuple3.of(2, "b2", 2),
                Tuple3.of(3, "c2", 3)
        );

        // 连接source1和source2
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);

        // 在并行度不为1的情况下，必须进行keyBy操作
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> keyByConnect = connect.keyBy(value -> value.f0, value -> value.f0);

        SingleOutputStreamOperator<String> process = keyByConnect.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            // 定义hashMap，用来存储数据
            Map<Integer, List<Tuple2<Integer, String>>> s1Map = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Map = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                if (!s1Map.containsKey(id)) {
                    // 1、 如key不存在，说明是第一条数据，初始化，put进map中
                    List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                    s1Values.add(value);
                    s1Map.put(id, s1Values);
                } else {
                    // 2、如key存在，则不是该key的第一条数据，直接添加到 value 的list中，
                    s1Map.get(id).add(value);
                }

                // 在source2中查找是否有能够匹配上的，如果能匹配上，则输出，反之则不输出
                if (s2Map.containsKey(id)) {
                    for (Tuple3<Integer, String, Integer> s2element : s2Map.get(id)) {
                        out.collect("s1=" + value + "<========>" + "s2=" + s2element);
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                if (!s2Map.containsKey(id)) {
                    // 1、 如key不存在，说明是第一条数据，初始化，put进map中
                    List<Tuple3<Integer, String, Integer>> s2Values = new ArrayList<>();
                    s2Values.add(value);
                    s2Map.put(id, s2Values);
                } else {
                    // 2、如key存在，则不是该key的第一条数据，直接添加到 value 的list中，
                    s2Map.get(id).add(value);
                }

                // 在source1中查找是否有能够匹配上的，如果能匹配上，则输出，反之则不输出
                if (s1Map.containsKey(id)) {
                    for (Tuple2<Integer, String> s1element : s1Map.get(id)) {
                        out.collect("s1=" + s1element + "<========>" + "s2=" + value);
                    }
                }
            }
        });

        process.print();

        env.execute();
    }
}
