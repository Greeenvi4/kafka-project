package producer1.service;

import producer1.entity.Invoice;
import producer1.repository.InvoiceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Сервис-потребитель для обработки зарегистрированных счетов из Kafka.
 *
 * <p>Данный сервис слушает топик {@code invoice-registered} и обновляет
 * статус счетов в базе данных после их успешной регистрации в системе.
 * Является частью Producer Service, которая завершает жизненный цикл счета
 * путем синхронизации статуса между Kafka и базой данных.</p>
 *
 * <p>Особенности работы:</p>
 * <ul>
 *   <li>Использует транзакционность {@link Transactional} для обеспечения
 *       атомарности операций с базой данных</li>
 *   <li>При получении сообщения проверяет существование счета в БД</li>
 *   <li>Обновляет статус счета на "REGISTERED" и сохраняет изменения</li>
 *   <li>В случае отсутствия счета выбрасывает исключение с сообщением об ошибке</li>
 * </ul>
 *
 * @author Andrus Gregory
 * @see Invoice
 * @see InvoiceRepository
 * @see KafkaListener
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceRegisteredConsumer {

    /**
     * Репозиторий для работы с сущностью {@link Invoice}.
     * Используется для поиска и обновления счетов в базе данных.
     * Внедряется через конструктор с помощью аннотации {@link RequiredArgsConstructor}.
     */
    private final InvoiceRepository invoiceRepository;

    /**
     * Обрабатывает входящие сообщения из топика {@code invoice-registered}.
     *
     * <p>Метод автоматически вызывается фреймворком Spring при поступлении
     * нового сообщения в топик. Аннотация {@link KafkaListener} обеспечивает
     * подписку на указанный топик с группой потребителей {@code producer-group-1}.</p>
     *
     * <p>Процесс обработки включает следующие шаги:</p>
     * <ol>
     *   <li>Логирование полученного счета для аудита и отладки</li>
     *   <li>Поиск существующего счета в базе данных по ID</li>
     *   <li>Обновление статуса счета на "REGISTERED"</li>
     *   <li>Сохранение обновленного счета в базе данных</li>
     *   <li>Логирование успешного обновления</li>
     * </ol>
     *
     * <p>Транзакционное поведение:</p>
     * <ul>
     *   <li>При успешном выполнении — изменения фиксируются в БД</li>
     *   <li>При возникновении исключения — транзакция откатывается,
     *       и сообщение будет обработано повторно (в зависимости от
     *       настроек Kafka)</li>
     * </ul>
     *
     * <p>Обработка ошибок:</p>
     * <ul>
     *   <li>Если счет не найден в базе данных — выбрасывается
     *       {@link RuntimeException} с детальным сообщением</li>
     *   <li>Исключение прерывает транзакцию и предотвращает
     *       некорректное обновление данных</li>
     * </ul>
     *
     * <p>Важные особенности:</p>
     * <ul>
     *   <li>Метод аннотирован {@link Transactional}, что гарантирует
     *       атомарность операций поиска и обновления</li>
     *   <li>Все операции логируются с использованием SLF4J для
     *       отслеживания потока обработки</li>
     *   <li>Ожидается, что входящий объект {@link Invoice} содержит
     *       заполненный идентификатор и статус "REGISTERED"</li>
     * </ul>
     *
     * @param invoice объект счета, полученный из Kafka.
     *
     * @throws RuntimeException если счет с указанным ID не найден в базе данных
     * @see KafkaListener
     * @see Transactional
     * @see InvoiceRepository#findById(Object)
     * @see InvoiceRepository#save(Object)
     */
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