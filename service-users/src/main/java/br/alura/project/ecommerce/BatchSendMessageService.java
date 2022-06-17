package br.alura.project.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users (" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200))");
        } catch (SQLException ex) {
            // be careful, the sql could be wrong,  be really careful
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        var batchService = new BatchSendMessageService();
        try (var service = new kafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                String.class,
                Map.of())) {
            service.run();
        }
    }

    private final kafkaDispatcher<User> userDispatcher = new kafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing New Batch");
        var message = record.value();
        System.out.println("Topic: " + message.getPayload());

        for (User user : getAllUsers()) {

            userDispatcher.send(message.getPayload(), user.getUuid(), user);
        }

    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users ").executeQuery();
        List<User> users = new ArrayList<>();

        while (results.next()) {
            users.add(new User(results.getString(1)));
        }

        return users;
    }
}
