package com.example.kafkademo.interceptor;

import com.example.kafkademo.demo.Constant;
import com.example.kafkademo.domain.User;
import com.example.kafkademo.serializer.CustomSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.example.kafkademo.demo.Constant.TOPIC_NAME;

/**
 * Created by wst on 2020/3/31.
 */
class CustomProducerInterceptorTest {

    @Test
    public void testInterceptor() {
        Properties properties = Constant.getKafkaProducerProperites();
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.example.kafkademo.interceptor.CustomProducerInterceptor");

        Serializer keySerializer = new StringSerializer();
        Serializer valueSerializer = new CustomSerializer();

        Producer<String, User> producer = new KafkaProducer<String, User>(properties, keySerializer, valueSerializer);


        User user = new User("Xi", "Hu", 33, "Hangzhou");
        ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC_NAME, user);

        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    @Test
    public void testInterceptorChain() {
        Properties properties = Constant.getKafkaProducerProperites();
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.example.kafkademo.interceptor.CustomProducerInterceptor");
        interceptors.add("com.example.kafkademo.interceptor.CustomProducerInterceptorChain");

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        Serializer keySerializer = new StringSerializer();
        Serializer valueSerializer = new CustomSerializer();

        Producer<String, User> producer = new KafkaProducer<String, User>(properties, keySerializer, valueSerializer);


        User user = new User("Xi", "Hu", 33, "Hangzhou");
        ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC_NAME, user);

        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();
    }

}