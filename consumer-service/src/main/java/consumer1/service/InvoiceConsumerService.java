package consumer1.service;

import consumer1.dto.InvoiceMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceConsumerService {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_CREATED = "invoice-created";
    private static final String TOPIC_REGISTERED = "invoice-registered";

    @KafkaListener(topics = TOPIC_CREATED, groupId = "consumer-group-1")
    public void consumeInvoice(InvoiceMessage message) {
        log.info("Received message from Kafka: {}", message);
        log.info("Processing invoice with ID: {}", message.getId());

        // Имитация обработки
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Меняем статус на REGISTERED
        message.setStatus("REGISTERED");
        log.info("Invoice {} status changed to REGISTERED", message.getId());

        // Отправляем обратно в Kafka
        kafkaTemplate.send(TOPIC_REGISTERED, message);
        log.info("Message sent back to Kafka topic '{}' for invoice: {}", TOPIC_REGISTERED, message.getId());
    }
}