package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Consumer {

    public interface Callback {
        void process(ConsumerRecord<String, String> record);
    }

    protected final KafkaConsumer<String, String> consumer;

    public Consumer(String server) throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "app");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "app_consumer");

        consumer = new KafkaConsumer<>(config);
    }

    public void listTopics() {
        System.out.println("Listando tópicos...");
        var topics = consumer.listTopics();
        for (Map.Entry<String, List<PartitionInfo>> found : topics.entrySet()) {
            System.out.println(found.getKey() + ": " + found.getValue());
        }
    }

    public void read(String topic, Callback callback) {
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
        records.forEach(callback::process);
    }
}