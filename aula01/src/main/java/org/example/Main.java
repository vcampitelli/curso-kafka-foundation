package org.example;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        final String server = "localhost:9092";
        final String topic = "acessos_site";

        if ((args.length > 0) && (args[0].equalsIgnoreCase("post"))) {
            post(server, topic);
            return;
        }

        consume(server, topic);
    }

    private static void post(String server, String topic) {
        try {
            ProducerGson p = new ProducerGson(server);
            p.send(topic, "/sobre", generateFakeIp(), (data, error) -> {
                if (error != null) {
                    System.err.println("Erro ao postar mensagem");
                    error.printStackTrace();
                    return;
                }

                System.out.println("Mensagem postada com sucesso");
                System.out.println("Partição: " + data.partition());
                System.out.println("Offset: " + data.offset());
                System.out.println("Timestamp: " + data.timestamp());
            });
        } catch (Exception err) {
            err.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * @link https://stackoverflow.com/a/9236244/2116392
     */
    private static String generateFakeIp() {
        Random r = new Random();
        return r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
    }

    private static void consume(String server, String topic) {
        try {
            ConsumerGson c = new ConsumerGson(server);
            c.listTopics();
            while (true) {
                c.read(topic, (record, parsedMessage) -> {
                    System.out.println("Mensagem recebida com sucesso");
                    System.out.println("Partição: " + record.partition());
                    System.out.println("Offset: " + record.offset());
                    System.out.println("Timestamp: " + record.timestamp());
//                    System.out.println("Conteúdo: " + record.value());
                    System.out.println("Conteúdo: " + parsedMessage);
                });
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception err) {
            err.printStackTrace();
            System.exit(1);
        }
    }
}
