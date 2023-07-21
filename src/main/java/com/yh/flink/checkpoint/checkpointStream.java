package com.yh.flink.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class checkpointStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        // 代码中用到HDFS，需要导入Hadoop依赖，指定访问HDFS的用户名
        //System.setProperty("HADOOP_USER_NAME","liangyuhang");

        /**
         * TODO 检查点配置：
         *
         */
        // 开启checkpoint；默认barrier对齐（EXACTLY_ONCE）
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        // 指定检查点的存储位置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("http://liangyuhangdemacbook-pro.local:9870/flink");
        // 指定超时时间：默认10分钟；该参数表示：超过设定时间
        checkpointConfig.setCheckpointTimeout(60000);
        // 指定同时运行checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        // 指定最小等待间隔：上一个与下一个checkpoint结束与开始之间的等待时间，设置>0,并发就回变为1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 取消作业时，checkpoint的数据 是否 保留在外部系统
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 容忍checkpoint故障的数量，连续失败的次数，默认时0
        checkpointConfig.setTolerableCheckpointFailureNumber(3);




        env.socketTextStream("127.0.0.1", 7777)
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> collector) -> {
                            String[] split = value.split(" ");
                            for (String word : split) {
                                collector.collect(Tuple2.of(word,1));
                            }
                        }).setParallelism(2)
                // Lambda表达式中，Java泛型擦出的存在，特殊处理
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute();
    }

}
