package com.yh.flink.api;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);

        /**
         * RichXXXFunction 富函数
         * 1、多了生命周期管理方法
         *  open()  每个子任务，在启动时，调用一次
         *  close() 每个子任务，在结束时，调用一次
         * 2、多了一个 运行时上下文
         *  可以获取一些运行时的环境信息
         *      ==》 如果是flink异常挂掉，不会调用close方法
         *      ==》 如果是正常调用cancel，可以调用close方法
         */
        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("子任务编号：" + getRuntimeContext().getIndexOfThisSubtask() +
                        "，子任务名称：" + getRuntimeContext().getTaskName()
                                + ",调用open方法");
            }
            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("子任务编号：" + getRuntimeContext().getIndexOfThisSubtask() +
                        "，子任务名称：" + getRuntimeContext().getTaskName()
                                + ",调用close方法");
            }
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });

        map.print();


        env.execute();
    }
}
