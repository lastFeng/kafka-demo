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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/4/1 11:11
 */
public class ConsumerWorker<K, V> implements Runnable {

    private final ConsumerRecords<K, V> records;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public ConsumerWorker(ConsumerRecords<K, V> records, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.records = records;
        this.offsets = offsets;
    }

    @Override
    public void run() {
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);

            // 处理消息，只进行打印
            partitionRecords.forEach(record ->
                System.out.println(String.format("thread=%s,topic=%s, partition=%d, offset=%d",
                    Thread.currentThread().getName(), record.topic(), record.partition(), record.offset()))
            );

            // 对偏移量进行处理
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

            synchronized (offsets) {
                // 如果不存在当前分区，那么添加入map中
                if (!offsets.containsKey(partition)) {
                    // 这里的偏移量必须是下一个要读取的数据
                    offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                } else {
                    long currentOffset = offsets.get(partition).offset();
                    // 读取当前分区没有到最新值，设置当前内容
                    if (currentOffset <= lastOffset) {
                        offsets.put(partition, new OffsetAndMetadata(currentOffset + 1));
                    }
                }
            }
        }
    }
}