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
package com.example.kafkademo.demo.consumer;

import com.example.kafkademo.demo.Constant;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.example.kafkademo.demo.Constant.LOOP_NUM;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/4/1 9:38
 */
public class ConsumerCommitTest {
    public static void main(String[] args) {
        String groupId = "123";
        Properties properties = Constant.getKakfaComsumerProperties(groupId);
        Deserializer deserializer = new StringDeserializer();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 创建consumer示例
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties, deserializer, deserializer);
        // 订阅topic
        consumer.subscribe(Arrays.asList("my-topic"));

        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        // 消费
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(10000);


                records.forEach(record -> {
                    System.out.printf("offset=%d, key=%s, value=%s%n", record.offset(),
                        record.key(), record.value());
                    buffer.add(record);
                });

                if (buffer.size() >= LOOP_NUM) {
                    consumer.commitAsync();
                    buffer.clear();
                }
            }
        } finally {
            consumer.close();
        }
    }
}