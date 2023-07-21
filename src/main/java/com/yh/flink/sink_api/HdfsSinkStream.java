package com.yh.flink.sink_api;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HdfsSinkStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);




        env.execute();
    }
}
