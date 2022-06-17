package br.alura.project.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try (var service = new kafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                Map.of())) {
            service.run();
        }
    }

    private final kafkaDispatcher<Order> orderDispatcher = new kafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing New Order, checking for Fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        var order = record.value();
        if (isFraud(order)) {
            // pretending that the fraud happens when the amount is >=4500
            System.out.println("Order is a fraud!!!" + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
        }
    }

    private boolean isFraud(Order order) {

        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}