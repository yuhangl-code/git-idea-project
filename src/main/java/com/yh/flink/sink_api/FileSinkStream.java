package com.yh.flink.sink_api;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class FileSinkStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 必须开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        // 输出到文件系统
        FileSink<String> fileSink = FileSink.<String>forRowFormat(
                        // 输出行式存储文件，指定路径、指定编码
                        new Path("/Users/liangyuhang/Documents/ideaProj/flink-utils/src/main/resources"),
                        new SimpleStringEncoder<>("UTF-8"))
                // (可选)输出文件名的 前缀、后缀
                .withOutputFileConfig(
                        OutputFileConfig
                                .builder()
                                .withPartPrefix("yuhang")
                                .withPartPrefix(".log")
                                .build()
                )
                // (可选)文件 分桶(意思是分目录，默认不指定按照小时)
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // (必须指定)文件滚动策略：（可以是时间、大小）  设置： 滚动10s、文件大小1M
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build()
                ).build();

        source.sinkTo(fileSink);

        env.execute();
    }
}
