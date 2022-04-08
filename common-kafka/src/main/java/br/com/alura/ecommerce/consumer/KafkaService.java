package br.com.alura.ecommerce.consumer;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.dispatcher.GsonSerializer;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction parse, Map<String, String> properties) {
        this(groupId, parse, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Map<String, String> properties) {
        this(groupId, parse, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction parse, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, properties));
    }

    public void run() throws ExecutionException, InterruptedException {
        KafkaDispatcher deadLetterDispatcher = new KafkaDispatcher<>();
        while(true) {
            ConsumerRecords<String, Message<T>> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty())
                continue;

            System.out.println("Encontrei " + records.count() + " regitros.");
            for (ConsumerRecord<String, Message<T>> record : records) {
                try {
                    parse.consume(record);
                } catch (Exception e) {
                    // only catches Exception because no matter which Exception
                    // I want to recover and parse the next one
                    // so far, just logging the exception for this message
                    e.printStackTrace();
                    Message message = record.value();
                    deadLetterDispatcher.send("ECOMMERCE_DEADLETTER",
                            message.getId().toString(),
                            message.getId().continueWith("DeadLetter"),
                            new GsonSerializer().serialize("", message));

                }
            }
        }
    }

    private Properties getProperties(String groupId, Map<String, String> overrideProperties) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
//        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

}
