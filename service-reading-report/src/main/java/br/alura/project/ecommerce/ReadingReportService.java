package br.alura.project.ecommerce;

import br.alura.project.ecommerce.consumer.ServiceRunner;
import br.alura.project.ecommerce.consumer.ConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadingReportService implements ConsumerService<User> {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) {
        new ServiceRunner(ReadingReportService::new).start(5);
    }

    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("------------------------------------------------");
        System.out.println("Processing report to" + record.value());
        var message = record.value();

        var user = message.getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File Created: " + target.getAbsolutePath());
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }
}
