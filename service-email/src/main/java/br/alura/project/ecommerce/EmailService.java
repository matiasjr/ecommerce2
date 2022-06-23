package br.alura.project.ecommerce;

import br.alura.project.ecommerce.consumer.ConsumerService;
import br.alura.project.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<String> {

    public static void main(String[] args) {

        new ServiceRunner(EmailService::new).start(5);
    }
    public String getConsumerGroup(){
        return EmailService.class.getSimpleName();
    }
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("------------------------------------------------");
        System.out.println("Sending E-mail.");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Email send.");
    }
}
