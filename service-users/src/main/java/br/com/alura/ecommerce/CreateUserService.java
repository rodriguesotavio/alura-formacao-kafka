package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase database;

    public static void main(String[] args) {
        new ServiceRunner(CreateUserService::new).start(1);
    }

    CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("create table Users(" +
                " uuid vachar(200) primary key," +
                " email varchar(200))");
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("---------------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println(record.value());
        Order order = record.value().getPayload();
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        final String uuid = UUID.randomUUID().toString();
        database.update("insert into Users (uuid, email) " +
                " values (?, ?)", uuid, email);
        System.out.println("Usuário: " + uuid + " E-mail: " + email + " adicionado.");
    }

    private boolean isNewUser(String email) throws SQLException {
        ResultSet results = database.query("select uuid from Users" +
                " where email = ? limit 1", email);
        return !results.next();
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
