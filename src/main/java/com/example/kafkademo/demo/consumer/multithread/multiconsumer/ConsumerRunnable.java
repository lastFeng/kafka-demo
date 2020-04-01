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
package com.example.kafkademo.demo.consumer.multithread.multiconsumer;

import com.example.kafkademo.demo.Constant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/4/1 13:42
 * 每个消费者执行的任务
 */
public class ConsumerRunnable<K, V> implements Runnable{

    private final KafkaConsumer<K, V> consumer;

    public ConsumerRunnable(String groupId, String topicName) {
        Properties properties = Constant.getKakfaComsumerProperties(groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");

        Deserializer deserializer = new StringDeserializer();
        this.consumer = new KafkaConsumer<K, V>(properties, deserializer, deserializer);
        this.consumer.subscribe(Arrays.asList(topicName));
    }

    /**
     * 一个消费者会对应一个partition，如果只有一个partition，那么只会有一个线程会读取数据，
     * 如果有多个partition，则会有多个线程执行
     */
    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<K, V> records = this.consumer.poll(Duration.ofMillis(1000));

                records.forEach(record ->
                    System.out.println(Thread.currentThread().getName() + " consumer:" + record.partition() +
                        " offset=" + record.offset())
                );
            }
        } finally {
            this.consumer.close();
        }

    }
}