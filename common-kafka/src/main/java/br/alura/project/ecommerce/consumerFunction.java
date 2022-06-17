package br.alura.project.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface consumerFunction<T> {
    void consume(ConsumerRecord<String, T> record) throws Exception;
}
