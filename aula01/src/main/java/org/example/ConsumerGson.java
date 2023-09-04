package org.example;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

public class ConsumerGson extends Consumer {
    public interface Callback extends Consumer.Callback {
        void process(ConsumerRecord<String, String> record, Map<String, String> parsedMessage);
    }

    private final Gson gson;

    private final TypeToken<Map<String, String>> mapType;

    public ConsumerGson(String server) throws Exception {
        super(server);
        gson = new Gson();
        mapType = new TypeToken<>() {
        };
    }

    public void read(String topic, Callback callback) {
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
        records.forEach(record -> {
            Map<String, String> parsedMessage = gson.fromJson(record.value(), mapType);
            callback.process(record, parsedMessage);
        });
    }
}