package com.example.kafkademo.serializer;

import com.example.kafkademo.demo.Constant;
import com.example.kafkademo.domain.User;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by wst on 2020/3/31.
 */
class CustomDeserializerTest {

    @Test
    public void testDeserializer() {

        String topic = "test-topic";
        Deserializer stringDeserializer = new StringDeserializer();
        Deserializer userDeserializer = new CustomDeserializer();

        Properties properties = Constant.getKakfaComsumerProperties("2");

        Consumer<String, User> consumer = new KafkaConsumer<String, User>(properties, stringDeserializer,
            userDeserializer);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                ConsumerRecords<String, User> records = consumer.poll(Integer.MAX_VALUE);

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