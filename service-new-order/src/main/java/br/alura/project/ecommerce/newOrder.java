package br.alura.project.ecommerce;

import br.alura.project.ecommerce.dispatcher.kafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class newOrder {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new kafkaDispatcher<Order>()) {
            var email = Math.random() + "@email.com";
            for (var i = 0; i < 10; i++) {

                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                var id = new CorrelationId(newOrder.class.getSimpleName());

                var order = new Order(orderId, amount, email);
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
                        id, order);

            }
        }
    }

}
