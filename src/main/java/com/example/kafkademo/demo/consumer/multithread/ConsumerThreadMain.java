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

import java.util.UUID;

import static com.example.kafkademo.demo.Constant.TOPIC_NAME;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/4/1 11:34
 */
public class ConsumerThreadMain {
    public static void main(String[] args) {
        String groupId = UUID.randomUUID().toString();

        final ConsumerThreadHandler<String, String> handler = new ConsumerThreadHandler<>(groupId, TOPIC_NAME);
        final int cpuCount = Runtime.getRuntime().availableProcessors();

        new Thread(() -> handler.consumer(cpuCount)).start();

        try {
            Thread.sleep(20000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Close the consumer...");
        handler.close();
    }
}