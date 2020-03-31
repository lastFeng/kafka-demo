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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.example.kafkademo.demo.Constant.LOOP_NUM;
import static com.example.kafkademo.demo.Constant.TOPIC_NAME;

/**
 * <p> Title: </p>
 *
 * <p> Description: </p>
 *
 * @author: Guo Weifeng
 * @version: 1.0
 * @create: 2020/3/30 10:23
 */
public class ProducerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = Constant.getKafkaProducerProperites();

        Serializer<String> keyValueSerializer = new StringSerializer();

        // 设置producer的属性, 显示配置key|value序列化于配置属性中
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 设置producer的属性，生成中配置key|value序列化
        Producer<String, String> producerShade = new KafkaProducer<String, String>(properties, keyValueSerializer, keyValueSerializer);

        // 同步发送数据
        for (int i = 0; i < LOOP_NUM; i++) {
            // 发送一个ProducerRecord， topic-key-value | topic-value （前者自己制定key，就会放入相应的partition中，而不指定key通过轮询均匀分布）
            RecordMetadata recordMetadata = producer.send(new ProducerRecord<>("my-topic", Integer.toString(i),
                Integer.toString(i))).get();
            // 可以对返回的结果进行操作，获取对应partition、offset等信息
        }

        // 异步发送数据
        for (int i = 0; i < LOOP_NUM; i++) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, Integer.toString(i), Integer.toString(i)),
                (metadata, exception) -> {
                // 发送成功与否只要判断exception是否为空即可
                if (exception == null) {
                    System.out.println("消息发送成功");
                } else {
                    if (exception instanceof RetriableException) {
                        // 处理可重复尝试的异常 -- 重试
                    } else {
                        // 不可重复的异常--记录日志--抛出相应内容
                    }
                }
            });
        }

        producer.close();
    }
}