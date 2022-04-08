package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        LogService logService = new LogService();

        Map<String, String> map = new HashMap<>();
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try(KafkaService service = new KafkaService(
                logService.getClass().getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                map)
        ) {
            service.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("---------------------------------------------");
        System.out.println("LOG: " + record.topic());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }

}
