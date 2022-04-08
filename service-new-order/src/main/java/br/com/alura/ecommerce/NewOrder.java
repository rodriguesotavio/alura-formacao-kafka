package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try (KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>()) {
            try (KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>()) {

                String userEmail = UUID.randomUUID().toString().substring(0, 8) + "@email.com";
                for (int i = 0; i < 10; i++) {
                    String orderId = UUID.randomUUID().toString();
                    BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);

                    Order order = new Order(orderId, amount, userEmail);

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail,
                            new CorrelationId(NewOrder.class.getSimpleName()),
                            order);

                    String emailBody = "Thank you for your order! We are processing your order!";
                    Email email = new Email("Assunto", emailBody);

                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userEmail,
                            new CorrelationId(NewOrder.class.getSimpleName()),
                            email);
                }
            }
        }
    }

}
