package com.yh.flink.basic_api;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        // 随机分区 :random.nextInt(下游算子并行度)
        //source.shuffle().print();
        // 轮询分区
        //source.rebalance().print();
        // rescale缩放：可实现轮询，局部组队，比reblance更高效
        //source.rescale().print();
        // broadcast广播；发送给下游所有的子任务
        //source.broadcast().print();
        //global 全局分区，所有数据都只发往第一个子任务
        //source.global().print();
        // keyby指定key进行分区，相同key会发往同一个子任务
        //source.keyBy()
        // one-to-one :forward分区器

        // 自定义分区
        DataStream<String> result = source.partitionCustom(new MyPartitioner(), new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        result.print();

        env.execute();
    }

    public static class MyPartitioner implements Partitioner<String> {
        @Override
        public int partition(String key, int numPartitions) {
            return Integer.parseInt(key) % numPartitions;
        }
    }
}
