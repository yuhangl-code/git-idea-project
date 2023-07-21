package com.yh.utils;

import com.yh.entity.MyData;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class createMyData {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<MyData> streamSource = env.addSource(new MySource());
        streamSource.print();


        env.execute();
    }

    public static class MySource implements SourceFunction<MyData> {
        // 定义标志位，用来控制数据的产生
        private boolean isRunning = true;
        private final Random random = new Random(0);

        @Override
        public void run(SourceContext<MyData> sourceContext) throws Exception {
            while (isRunning) {
//                sourceContext.collect(
//                        new MyData(FakerUil.randomNum(1,150), Long.valueOf(FakerUil.randomTime("2022-01-01 00:00:00","2022-04-01 00:00:00")), FakerUil.randomNum(100,1500))
//                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
