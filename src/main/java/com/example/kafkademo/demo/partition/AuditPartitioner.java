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
package com.example.kafkademo.demo.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/3/30 14:33
 */
public class AuditPartitioner implements Partitioner {
    private Random random = new Random();

    /**
     * 进行资源的初始化
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("AuditPartitioner");
    }

    /**
     * 设置消息发送的位置，返回的int值即为消息将要发送的分区位置
     * 这边的需求为：
     *   如果key中没有“audit”字段，就随机分配分区，如果存在，则将key对应的值保存在最后一个分区位置
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        String auditKey = (String) key;
        // 通过cluster获取相应topic的所有分区
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        int auditPartition = partitionInfos.size() - 1;

        return ((auditKey == null || auditKey.isEmpty() || auditKey.contains("audit")) && auditPartition != 0) ? random.nextInt(auditPartition) : auditPartition;
    }

    @Override
    public void close() {
        // 实现资源的必要清理
    }
}