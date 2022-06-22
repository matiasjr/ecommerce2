package br.alura.project.ecommerce.consumer;

import br.alura.project.ecommerce.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {

    //you may arqgue that a ConsumerException would be better
    // and its ok, ot can be better
    void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
    String getTopic();
    String getConsumerGroup();

}
