package com.yh.flink.watermark_api;

import com.yh.entity.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class waterMarkStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                });


        // 1、定义watermark策略
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                // TODO 升序watermark，没有等待时间   .<WaterSensor>forMonotonousTimestamps()
                // TODO 乱序，有等待时间(等待3s)
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定时间戳分配器，从数据中提取
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        // 返回时间戳，毫秒
                        System.out.println("数据=" + element + "；records=" + recordTimestamp);
                        return element.getTs() * 1000L;
                    }
                })
                //设置空闲等待时间（用于多条并行度的情况下）
                //.withIdleness()
                ;

        // 2、指定watermark策略
        SingleOutputStreamOperator<WaterSensor> watermarkSensorDS = source.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        watermarkSensorDS.keyBy(sensor -> sensor.getId())
                // 使用EventTime事件时间，watermark才能起作用
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //.allowedLateness(Time.seconds(2))  // 允许延迟2s，关窗
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                            long startTS = context.window().getStart();
                            long endTS = context.window().getEnd();
                            String startFormat = DateFormatUtils.format(startTS, "yyyy-MM-dd HH:mm:ss");
                            String endFormat = DateFormatUtils.format(endTS, "yyyy-MM-dd HH:mm:ss");

                            long count = elements.spliterator().estimateSize();
                            out.collect("key=" + s + "的窗口；" + "[ " + startFormat + "," + endFormat + " ]包含" + count + "条数据====》"+ elements.toString());
                        }
                }).print();


        env.execute();
    }
}
