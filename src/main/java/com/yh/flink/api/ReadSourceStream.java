package com.yh.flink.api;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadSourceStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 从文件中读取数据
         *
             FileSource<String> fileSource = FileSource.forRecordStreamFormat(
             new TextLineInputFormat(),
             new Path("/Users/liangyuhang/Documents/ideaProj/flink-utils/src/main/resources/words.txt")
             ).build();

         */


        // 从Kafka读取
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")  // 指定kafka节点的地址和端口
                .setGroupId("flink-kafka")  // 指定消费者组的ID
                .setTopics("demo")        // 指定消费者的topic
                .setValueOnlyDeserializer(new SimpleStringSchema())  // 指定反序列化value
                .setStartingOffsets(OffsetsInitializer.latest())    // Flink消费kafka的策略
                .build();


        /**
         * 新source写法：
         *  Source的实现类、WaterMarks，名字
         */
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "source")
                .print();


        env.execute();
    }
}

/**
 *  kafka消费者的参数：
 *  auto.reset.offsets
 *      earliest : 如果有offset，从offset开始消费，如果没有offset，从 最早 开始消费
 *      latest   : 如果有offset，从offset开始消费，如果没有offset，从 最新 开始消费
 *
 *  flink的Kafka source： offset消费策略：OffsetsInitializer 默认是earliest
 *      earliest：   一定从 最早 开始消费
 *      latest  ：   一定从 最新 开始消费
 *
 *
 */
