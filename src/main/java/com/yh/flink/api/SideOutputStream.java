package com.yh.flink.api;

import com.yh.entity.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777)
            .map(new MapFunction<String, WaterSensor>() {
                @Override
                public WaterSensor map(String value) throws Exception {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                }
            });

        // 创建output对象；第一个参数：标签名；第二个参数：放入侧输出流中的 数据类型（必须是Typeinformation）
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        /**
         *  ProcessFunction这里的输出类型指的是主流的输出类型
         *  侧输出流用到了Context对象
          */
        SingleOutputStreamOperator<WaterSensor> processDS = sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.getId();
                if ("s1".equals(id)) {
                    /**
                     *  上下文调用output，将数据放入侧输出流
                     *  第一个参数：Tag对象
                     *  第二个参数：要放入的数据
                     */
                    ctx.output(s1Tag, value);
                } else if ("s2".equals(id)) {
                    ctx.output(s2Tag, value);
                } else {
                    out.collect(value);
                }
            }
        });
        // 打印侧输出流s1
        processDS.getSideOutput(s1Tag).printToErr("s1");
        // 打印侧输出流s2
        processDS.getSideOutput(s2Tag).printToErr("s2");
        // 打印的是主流的数据
        processDS.print("主流");

        env.execute();
    }
}
