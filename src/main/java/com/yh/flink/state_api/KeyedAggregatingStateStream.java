package com.yh.flink.state_api;

import com.yh.entity.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedAggregatingStateStream {

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

                    AggregatingState<Integer,Double> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        aggregatingState = getRuntimeContext()
                                .getAggregatingState(
                                        new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>(
                                                "aggregatingState"
                                                ,new AggregateFunction<Integer, Tuple2<Integer,Integer>, Double>() {

                                                    @Override
                                                    public Tuple2<Integer, Integer> createAccumulator() {
                                                        return Tuple2.of(0,0);
                                                    }

                                                    @Override
                                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                        return Tuple2.of(accumulator.f0+value,accumulator.f1+1);
                                                    }

                                                    @Override
                                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                        return accumulator.f0 * 1D / accumulator.f1;
                                                    }

                                                    @Override
                                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                        return Tuple2.of( a.f0+b.f0, a.f1+b.f1 );
                                                    }},
                                                Types.TUPLE(Types.INT,Types.INT)));
                    };

                    @Override
                    public void processElement(WaterSensor sensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        // 将 水位值 添加到 状态中
                        aggregatingState.add(sensor.getVc());

                        collector.collect("传感器id="+sensor.getId()+";累计的水位值综合="+aggregatingState.get());
                    }
                })
                        .print();

        env.execute();
    }
}
