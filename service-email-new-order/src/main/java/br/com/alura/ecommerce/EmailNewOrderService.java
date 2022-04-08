package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) {
        new ServiceRunner(EmailNewOrderService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("---------------------------------------------");
        System.out.println("Processing new order, preparing e-mail");
        System.out.println(record.value());

        Message<Order> message = record.value();
        Order order = message.getPayload();

        String emailBody = "Thank you for your order! We are processing your order!";

        CorrelationId id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());

        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, emailBody);
    }

    @Override
    public String getConsumerGroup() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }
}
