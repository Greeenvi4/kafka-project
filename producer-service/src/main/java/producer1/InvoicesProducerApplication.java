package producer1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class InvoicesProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(InvoicesProducerApplication.class, args);
    }
}