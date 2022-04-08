package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users(" +
                    " uuid vachar(200) primary key," +
                    " email varchar(200))");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        BatchSendMessageService batchService = new BatchSendMessageService();
        try (KafkaService service = new KafkaService(
                batchService.getClass().getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                new HashMap<>())
        ) {
            service.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void parse(ConsumerRecord<String, Message<String>> record)
            throws ExecutionException, InterruptedException, SQLException {

        final Message<String> message = record.value();
        final String payload = message.getPayload();

        System.out.println("---------------------------------------------");
        System.out.println("Processing new batch");
        System.out.println("Tópico: " + payload);

        for (User user : getAllUsers()) {
            userDispatcher.sendAsync(payload, user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user);
            System.out.println("Enviando para " + user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        ResultSet results = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>();
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }
        return users;
    }
}
