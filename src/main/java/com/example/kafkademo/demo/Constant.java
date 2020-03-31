/*
 * Copyright 2001-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.kafkademo.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/3/30 13:39
 */
public class Constant {
    public static final String TOPIC_NAME = "my-topic";
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String KEY_VALUE_CLASS_NAME = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KEY_VALUE_STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String ACK_TYPE = "-1";
    public static final String COMPRESS_TYPE = "lz4";
    public static final int LOOP_NUM = 100;
    public static final String CUSTOM_PARTITON_CLASS = "com.example.kafkademo.demo.partition.AuditPartitioner";
    public static final String ENCODING = "UTF8";

    public static Properties getKafkaProducerProperites() {
        Properties properties = new Properties();
        // 指定kafka服务
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        // 制定key:value的序列化格式
        //properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_VALUE_CLASS_NAME);
        //properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KEY_VALUE_CLASS_NAME);

        // 副本同步，需要全部同步，并且leader接收到ack才提交
        properties.put(ProducerConfig.ACKS_CONFIG, ACK_TYPE);
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);

        // 自定义的分区机制
        //properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CUSTOM_PARTITON_CLASS);

        // 压缩类型
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESS_TYPE);
        return properties;
    }

    public static Properties getKakfaComsumerProperties(String groupId) {
        StringDeserializer stringDeserializer = new StringDeserializer();
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 从最早的消息开始读取
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_VALUE_STRING_DESERIALIZER);
        //properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KEY_VALUE_STRING_DESERIALIZER);

        return properties;
    }
}