package com.yh.flink.state_api;

import com.yh.entity.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

public class KeyedMapStateStream {

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

                    MapState<Integer,Integer> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("mapState",Types.INT,Types.INT));
                    }
                    @Override
                    public void processElement(WaterSensor sensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        // 1、判断是否对应的key
                        Integer vc = sensor.getVc();
                        if (mapState.contains(vc)) {
                            // 如果包含vc的key，直接对vc+1
                            Integer countvc = mapState.get(vc);
                            countvc ++;
                            mapState.put(vc,countvc);
                        } else {
                            // 如果不包含vc的key，初始化key
                            mapState.put(vc,1);
                        }
                        // 2、遍历map状态，输出每个k-v的值
                        StringBuffer outStr = new StringBuffer();
                        outStr.append("传感器id="+sensor.getId()+"\n");
                        for (Map.Entry<Integer, Integer> vcCount : mapState.entries()) {
                            outStr.append(vcCount.toString()+"\n");
                        }
                        outStr.append("==================");

                        collector.collect(outStr.toString());
                    }
                })
                        .print();

        env.execute();
    }
}
