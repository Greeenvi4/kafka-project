package producer1.service;

import producer1.dto.CreateInvoiceRequest;
import producer1.entity.Invoice;
import producer1.repository.InvoiceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceService {
    private final InvoiceRepository invoiceRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_CREATE_INVOICE = "invoice-created";

    @Transactional
    public Invoice createInvoice(CreateInvoiceRequest request) {
        // 1. Сохраняем в БД со статусом NEW
        Invoice invoice = new Invoice();
        invoice.setAccountNumber(request.getAccountNumber());
        invoice.setAmount(request.getAmount());
        invoice.setStatus("NEW");
        invoice.setCreatedAt(LocalDateTime.now());

        Invoice saved = invoiceRepository.save(invoice);
        log.info("Invoice saved with status NEW: {}", saved.getId());

        // 2. Отправляем сообщение в Kafka
        kafkaTemplate.send(TOPIC_CREATE_INVOICE, saved);
        log.info("Message sent to Kafka for invoice: {}", saved.getId());

        return saved;
    }
}