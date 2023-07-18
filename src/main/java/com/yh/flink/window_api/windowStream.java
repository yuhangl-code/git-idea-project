package com.yh.flink.window_api;

import com.yh.entity.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class windowStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 必须开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                });

        KeyedStream<WaterSensor, String> keyedStream = source.keyBy(sensor -> sensor.getId());


        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

//        SingleOutputStreamOperator<WaterSensor> reduce = windowedStream.reduce(new ReduceFunction<WaterSensor>() {
//            @Override
//            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
//                System.out.println("调用reduce方法，value1=" + value1 + ";value2=" + value2);
//                return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
//            }
//        });

//        SingleOutputStreamOperator<String> aggregate = windowedStream.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
//            @Override
//            public Integer createAccumulator() {
//                System.out.println("创建累加器");
//                return 0;
//            }
//
//            @Override
//            public Integer add(WaterSensor value, Integer accumulator) {
//                System.out.println("调用add方法，聚合逻辑");
//                return value.getVc() + accumulator;
//            }
//
//            @Override
//            public String getResult(Integer accumulator) {
//                System.out.println("调用getResult方法，获取最终结果，触发时输出");
//                return accumulator.toString();
//            }
//
//            @Override
//            public Integer merge(Integer a, Integer b) {
//                System.out.println("调用merge方法，只有会话窗口才会用到");
//                return null;
//            }
//        });

        SingleOutputStreamOperator<String> process = windowedStream.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long startTS = context.window().getStart();
                long endTS = context.window().getEnd();
                String startFormat = DateFormatUtils.format(startTS, "yyyy-MM-dd HH:mm:ss");
                String endFormat = DateFormatUtils.format(endTS, "yyyy-MM-dd HH:mm:ss");

                long count = elements.spliterator().estimateSize();
                out.collect("key=" + s + "的窗口；" + "[ " + startFormat + "," + endFormat + " ]包含" + count + "条数据====》"+ elements.toString());
            }
        });


        env.execute();
    }
}


// 1、指定窗口的分配器：指定用哪一种窗口
// 1.1 没有keyby的窗口：所有数据都在 同一个子任务中，并行度只能为1
// 基于时间的
//source.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))       滚动窗口
//source.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2))    滑动窗口
//source.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))   会话窗口

// 基于计数的
//source.countWindowAll(5)
//source.countWindowAll(5,2)
//source.windowAll(GlobalWindows.create())  全局窗口：计数窗口的底层，需要自定义触发器

// 2、指定窗口函数： 窗口内数据的计算逻辑
//AllWindowedStream<WaterSensor, TimeWindow> windowedStream = source.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));
// 2.1 增量聚合函数:来一条算一条；窗口触发的时候输出计算结果
//     全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果