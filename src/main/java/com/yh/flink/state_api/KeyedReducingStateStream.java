package com.yh.flink.state_api;

import com.yh.entity.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

public class KeyedReducingStateStream {

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

                    ReducingState<Integer> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        reducingState = getRuntimeContext()
                                .getReducingState(new ReducingStateDescriptor<Integer>("reducingState",
                                        new ReduceFunction<Integer>() {
                                            @Override
                                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                                return value1+value2;
                                            }
                                        }
                                        ,Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor sensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        // 来一条数据，添加到状态中
                        reducingState.add(sensor.getVc());

                        collector.collect("传感器id="+sensor.getId()+";累计的水位值综合="+reducingState.get());
                    }
                })
                        .print();

        env.execute();
    }
}
