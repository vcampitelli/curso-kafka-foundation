package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;

public class Producer {
    private final KafkaProducer<String, String> producer;

    public Producer(String server) throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "app");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);

        producer = new KafkaProducer<>(config);
    }

    public void send(String topic, String message, Callback callback) throws Exception {
        producer.send(
                new ProducerRecord<>(topic, message),
                callback
        ).get();
    }
}