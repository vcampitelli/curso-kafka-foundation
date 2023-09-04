package org.example;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Callback;

import java.util.LinkedHashMap;
import java.util.Map;

public class ProducerGson extends Producer {
    private final Gson gson;

    public ProducerGson(String server) throws Exception {
        super(server);
        gson = new Gson();
    }

    public void send(String topic, String page, String ip, Callback callback) throws Exception {
        Map<String, String> stringMap = new LinkedHashMap<>();
        stringMap.put("page", page);
        stringMap.put("ip", ip);

        String message = gson.toJson(stringMap);
        super.send(topic, message, callback);
    }
}