package com.yh.flink.state_api;

import com.yh.entity.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedValueStateStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sourceDS = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000L;
                                    }
                                })
                );

        sourceDS.keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    // TODO 定义状态
                    ValueState<Integer> lastState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 在open方法中初始化状态
                        // 状态描述器：两个参数：名字；存储的类型
                        lastState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor sensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        // 1、取出上一条数据的水位值
                        int lastVc = lastState.value() == null? 0: lastState.value();

                        // 2、求差值的绝对值，判断是否超过10
                        if ( Math.abs(sensor.getVc() - lastVc) > 10 ) {
                            collector.collect("传感器id="+sensor.getId()+";当前水位值="+sensor.getVc()+",与上一条水位值="+ lastVc+ ",相差超过10!!!");
                        }

                        // 3、更新状态里的水位值
                        lastState.update(sensor.getVc());
                    }
                })
                        .print();

        env.execute();
    }
}
