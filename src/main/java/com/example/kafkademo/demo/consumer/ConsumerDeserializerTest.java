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
import com.example.kafkademo.domain.User;
import com.example.kafkademo.serializer.CustomDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/3/31 15:58
 */
public class ConsumerDeserializerTest {
    public static void main(String[] args) {
        String topic = "test-topic";
        Deserializer stringDeserializer = new StringDeserializer();
        Deserializer userDeserializer = new CustomDeserializer();

        Properties properties = Constant.getKakfaComsumerProperties("23");

        Consumer<String, User> consumer = new KafkaConsumer<String, User>(properties, stringDeserializer,
            stringDeserializer);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                ConsumerRecords<String, User> records = consumer.poll(1000);

                records.forEach(record ->
                    System.out.printf("offset=%d, key=%s, value=%s%n", record.offset(),
                        record.key(), record.value())
                );
            }
        } finally {
            consumer.close();
        }
    }
}