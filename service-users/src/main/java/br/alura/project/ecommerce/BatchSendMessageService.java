package br.alura.project.ecommerce;

import br.alura.project.ecommerce.consumer.kafkaService;
import br.alura.project.ecommerce.dispatcher.kafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                Map.of())) {
            service.run();
        }
    }

    private final kafkaDispatcher<User> userDispatcher = new kafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing New Batch");
        var message = record.value();
        System.out.println("Topic: " + message.getPayload());

        //if(true) throw new RuntimeException("deu um erro que eu forcei");

        for (User user : getAllUsers()) {

            userDispatcher.sendAsync(message.getPayload(), user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user);
            System.out.println("Acho que enviei para " + user);
        }

    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>();

        while (results.next()) {
            users.add(new User(results.getString(1)));
        }

        return users;
    }
}
