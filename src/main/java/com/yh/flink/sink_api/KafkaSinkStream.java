package com.yh.flink.sink_api;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class KafkaSinkStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 必须开启checkpoint,否则 精准一次 无法写入Kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<String> source = env.socketTextStream("localhost", 7777);

        /**
         * kafka sink
         * TODO 注意：如果要 精准一次写入 Kafka， 需要满足以下条件，缺一不可
         * 1、必须要开启checkpoint
         * 2、必须设置一个 事务前缀
         * 3、必须设置一个 超时时间   checkpoint间隔 <  xx < 15分钟
          */

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定Kafka的地址和端口
                .setBootstrapServers("localhost:9092")
                // 指定数据序列化器
                .setRecordSerializer(
                        /** 未指定key的情况下：
                         *  KafkaRecordSerializationSchema.<String>builder()
                         *                                 .setTopic("demo")
                         *                                 .setValueSerializationSchema(new SimpleStringSchema())
                         *                                 .build()
                         */

                        /** 指定Kafka key，可自定义反序列化器
                         * 实现 一个接口，重写序列化方法
                         * 指定key， 转成字节数组
                         * 指定value，转成字节数组
                         * 返回 ProducerRecord对象
                         */
                        new KafkaRecordSerializationSchema<String>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                String[] datas = element.split(" ");
                                byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("demo",key,value);
                            }
                        }
                )
                // 写到Kafka的一次性级别：精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置 事务的前缀
                .setTransactionalIdPrefix("yuhang-")
                // 如果是精准一次，必须设置 事务超时时间：大于checkpoint间隔，小于 max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*60*1000+"")
                .build();

        source.sinkTo(kafkaSink);

        env.execute();
    }
}
