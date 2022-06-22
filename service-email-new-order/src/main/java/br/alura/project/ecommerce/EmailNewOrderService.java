package br.alura.project.ecommerce;

import br.alura.project.ecommerce.consumer.ConsumerService;
import br.alura.project.ecommerce.consumer.ServiceRunner;
import br.alura.project.ecommerce.dispatcher.kafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    public static void main(String[] args) {
        new ServiceRunner(EmailNewOrderService::new).start(1);
    }

    private final kafkaDispatcher<String> emailDispatcher = new kafkaDispatcher<>();

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
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

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

}
