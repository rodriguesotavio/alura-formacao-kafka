package br.com.alura.ecommerce.dispatcher;

import br.com.alura.ecommerce.CorrelationId;
import br.com.alura.ecommerce.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, T> producer;

    public KafkaDispatcher() {
        producer = new KafkaProducer<>(properties());
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId correlationId, T payload) {
        Message value = new Message<T>(correlationId.continueWith("_" + topic), payload);
        ProducerRecord record = new ProducerRecord<>(topic, key, value);

        Callback callback = (RecordMetadata data, Exception e) -> {
            if (e != null) {
                e.printStackTrace();
                return;
            }
            System.out.println("::: topic " + data.topic() + " ::: partition " + data.partition()
                    + " ::: offset " + data.offset() + " ::: timestamp " + data.timestamp());
        };

        return producer.send(record, callback);
    }

    public void send(String topic, String key, CorrelationId correlationId, T payload)
            throws ExecutionException, InterruptedException {
        sendAsync(topic, key, correlationId, payload).get();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
