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

import java.util.ArrayList;
import java.util.List;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/4/1 13:50
 * 消费线程类管理类
 */
public class ConsumerGroup<K, V> {
    private List<ConsumerRunnable<K, V>> consumers = new ArrayList<>();

    public ConsumerGroup(int consumerNum, String groupId, String topicName) {
        for (int i = 0; i < consumerNum; i++) {
            ConsumerRunnable<K, V> consumerRunnable = new ConsumerRunnable<K, V>(groupId, topicName);
            consumers.add(consumerRunnable);
        }
    }

    public void execute() {
        this.consumers.forEach(consumer -> new Thread(consumer).start());
    }
}