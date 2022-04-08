package br.com.alura.ecommerce.consumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class ServiceProvider<T> implements Callable<Void> {

    private final ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    public Void call() throws Exception {
        ConsumerService service = factory.create();
        try(KafkaService kafkaService = new KafkaService(
                service.getConsumerGroup(),
                service.getTopic(),
                service::parse,
                new HashMap<>())
        ) {
            kafkaService.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
