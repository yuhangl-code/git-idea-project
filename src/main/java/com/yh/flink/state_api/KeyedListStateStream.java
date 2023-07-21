package com.yh.flink.state_api;

import com.yh.entity.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

public class KeyedListStateStream {

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

                    ListState<Integer> vcListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState",Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor sensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        // 1、来一条数据，存入状态中
                        vcListState.add(sensor.getVc());

                        // 2、从list中取出数据，拷贝到一个list中排序，取出前3（状态中只留3条最大的数据）
                        Iterable<Integer> vcListIt = vcListState.get();
                        ArrayList<Integer> vcList  = new ArrayList<>();
                        for (Integer vc : vcListIt) {
                            // 拷贝到新的arraylist中
                            vcList.add(vc);
                        }
                        // 对list进行降序排序
                        vcList.sort((o1,o2) -> o2-o1);
                        // 只保留最大的3条数据
                        if (vcList.size()>3) {
                            vcList.remove(3);
                        }

                        collector.collect("传感器id="+sensor.getId()+";最大的3个水位值="+vcList.toString());

                        // 3、更新 list 状态
                        vcListState.update(vcList);
                    }
                })
                        .print();

        env.execute();
    }
}
