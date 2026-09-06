package producer1.service;

import producer1.entity.Invoice;
import producer1.repository.InvoiceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceRegisteredConsumer {
    private final InvoiceRepository invoiceRepository;

    @KafkaListener(topics = "invoice-registered", groupId = "producer-group-1")
    @Transactional
    public void consumeRegisteredInvoice(Invoice invoice) {
        log.info("Received registered invoice from Kafka: {}", invoice);

        // Обновляем статус в БД
        Invoice existingInvoice = invoiceRepository.findById(invoice.getId())
                .orElseThrow(() -> new RuntimeException("Invoice not found: " + invoice.getId()));

        existingInvoice.setStatus("REGISTERED");
        invoiceRepository.save(existingInvoice);

        log.info("Invoice {} status updated to REGISTERED in database", invoice.getId());
    }
}