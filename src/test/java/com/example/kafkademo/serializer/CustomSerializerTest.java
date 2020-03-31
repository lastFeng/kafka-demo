package com.example.kafkademo.serializer;

import com.example.kafkademo.demo.Constant;
import com.example.kafkademo.domain.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by wst on 2020/3/31.
 */
class CustomSerializerTest {
    @Test
    public void testSerializer() throws ExecutionException, InterruptedException {
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
    }
}