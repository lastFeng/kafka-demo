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
package com.example.kafkademo.demo.producer;

import com.example.kafkademo.demo.Constant;
import com.example.kafkademo.domain.User;
import com.example.kafkademo.serializer.CustomSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/3/31 15:57
 */
public class ProducerSerializerTest {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = Constant.getKafkaProducerProperites();

        Serializer keySerializer = new StringSerializer();
        Serializer valueSerializer = new CustomSerializer();
        Producer<String, User> producer = new KafkaProducer<String, User>(properties, keySerializer, valueSerializer);

        String myTopic = "test-topic";
        User user = new User("Xi", "Hu", 33, "Hangzhou");
        ProducerRecord<String, User> producerRecord = new ProducerRecord(myTopic, user);
        for (int i = 0; i < 2 ; i++) {
            producer.send(producerRecord, (m,e) -> {
                if (e == null) {
                    System.out.println("发送成功");
                } else {
                    System.out.println("发送失败");
                }
            });
        }
        Thread.sleep(2000);
    }
}