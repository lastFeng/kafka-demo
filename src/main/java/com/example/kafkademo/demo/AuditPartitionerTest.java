package com.example.kafkademo.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

import static com.example.kafkademo.demo.Constant.TOPIC_NAME;

/**
 * Created by wst on 2020/3/30.
 */
public class AuditPartitionerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Producer<String, String> producer = new KafkaProducer<String, String>(Constant.getKafkaProducerProperites());
        ProducerRecord noKeyRecord = new ProducerRecord(TOPIC_NAME, "non-key record");
        ProducerRecord auditRecord = new ProducerRecord(TOPIC_NAME, "audit", "audit record");
        ProducerRecord noAuditRecord = new ProducerRecord(TOPIC_NAME, "other", "no audit record");

        producer.send(noKeyRecord).get();
        producer.send(noKeyRecord).get();
        producer.send(auditRecord).get();
        producer.send(noAuditRecord).get();
        producer.send(noAuditRecord).get();

        producer.close();
    }

}