package br.alura.project.ecommerce;

import br.alura.project.ecommerce.consumer.kafkaService;
import br.alura.project.ecommerce.dispatcher.kafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var fraudService = new EmailNewOrderService();
        try (var service = new kafkaService<>(EmailNewOrderService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Map.of())) {
            service.run();
        }
    }

    private final kafkaDispatcher<String> emailDispatcher = new kafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing New Order, preparing email");
        var message = record.value();
        System.out.println(record.value());

        var order = message.getPayload();
        var emailCode = "Thank you for your Order!!!. We are processing your order.";
        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(),
                id, emailCode);
    }

}
