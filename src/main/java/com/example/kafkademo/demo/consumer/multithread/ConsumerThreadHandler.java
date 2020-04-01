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
package com.example.kafkademo.demo.consumer.multithread;

import com.example.kafkademo.demo.Constant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/4/1 10:48
 * 一个consumer下有多个线程来操作：是否可以？
 * consumer多线程管理类
 */
public class ConsumerThreadHandler<K, V> {
    private final KafkaConsumer<K, V> kafkaConsumer;
    private ExecutorService service;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    /**
     * 构建消费者
     */
    public ConsumerThreadHandler(String groupId, String topicName) {
        Properties properties = Constant.getKakfaComsumerProperties(groupId);
        // 非自动提交commit，由多线程计算提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Deserializer deserializer = new ByteArrayDeserializer();
        this.kafkaConsumer = new KafkaConsumer<K, V>(properties, deserializer, deserializer);

        this.kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
            /**
             * 此处consumer提交偏移
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                kafkaConsumer.commitSync(offsets);
            }

            /**分区分配完之后，清除偏移内容*/
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                offsets.clear();
            }
        });
    }

    /**
     * 消费主方法
     * @param threadNum 线程数
     */
    public void consumer(int threadNum) {
        service = new ThreadPoolExecutor(threadNum, threadNum, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while (true) {
                ConsumerRecords<K, V> records = this.kafkaConsumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    service.submit(new ConsumerWorker(records, offsets));
                }

                // 提交offset
                commitOffsets();
            }
        } finally {
            commitOffsets();
            this.kafkaConsumer.close();
        }
    }

    /**
     * 提交offset
     */
    private void commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> unmodifiedMap = null;

        synchronized (offsets) {
            if (offsets.isEmpty()) {
                return;
            }

            unmodifiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            offsets.clear();
        }

        this.kafkaConsumer.commitSync(unmodifiedMap);
    }

    public void close() {
        this.kafkaConsumer.wakeup();
        service.shutdown();
    }
}