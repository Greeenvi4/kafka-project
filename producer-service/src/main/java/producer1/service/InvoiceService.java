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

/**
 * Сервис для управления счетами и их публикации в Kafka.
 *
 * <p>Основной сервис Producer Service, отвечающий за создание новых счетов,
 * сохранение их в базе данных и отправку событий в Kafka для асинхронной
 * обработки другими микросервисами.</p>
 * <p>Бизнес-процесс создания счета:</p>
 * <ol>
 *   <li>Получение данных от клиента через DTO {@link CreateInvoiceRequest}</li>
 *   <li>Создание сущности {@link Invoice} с начальным статусом "NEW"</li>
 *   <li>Сохранение счета в базе данных</li>
 *   <li>Публикация события в Kafka топик {@value #TOPIC_CREATE_INVOICE}</li>
 *   <li>Возврат созданного счета клиенту</li>
 * </ol>
 *
 * <p>Транзакционное поведение:</p>
 * <ul>
 *   <li>Метод {@link #createInvoice(CreateInvoiceRequest)} аннотирован
 *       {@link Transactional}, что гарантирует атомарность операций</li>
 *   <li>При успешном выполнении — счет сохраняется в БД и отправляется в Kafka</li>
 *   <li>При возникновении исключения — транзакция откатывается, и счет
 *       не сохраняется в базе данных</li>
 * </ul>
 *
 * <p><b>Важное замечание:</b> Отправка в Kafka не является частью транзакции базы данных.
 * В случае ошибки отправки сообщения в Kafka, счет останется в базе данных со статусом "NEW",
 * что может привести к несогласованности данных.
 *
 * @author Andrus Gregory
 * @see Invoice
 * @see InvoiceRepository
 * @see KafkaTemplate
 * @see CreateInvoiceRequest
 * @see producer1.controller.InvoiceController
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceService {

    /**
     * Репозиторий для работы с сущностью {@link Invoice}.
     * Используется для сохранения и поиска счетов в базе данных.
     * Внедряется через конструктор с помощью аннотации {@link RequiredArgsConstructor}.
     */
    private final InvoiceRepository invoiceRepository;

    /**
     * Шаблон для отправки сообщений в Kafka.
     * Используется для публикации событий о создании новых счетов.
     *
     * <p>Типизирован как {@code KafkaTemplate<String, Object>}, где:
     * <ul>
     *   <li>String — тип ключа сообщения (обычно null или ID)</li>
     *   <li>Object — тип значения (в данном случае {@link Invoice})</li>
     * </ul>
     * </p>
     */
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Топик Kafka для отправки сообщений о созданных счетах.
     * В этот топик публикуются счета со статусом "NEW" для дальнейшей
     * асинхронной обработки сервисом-потребителем.
     *
     */
    private static final String TOPIC_CREATE_INVOICE = "invoice-created";

    /**
     * Создает новый счет на основе данных запроса и публикует событие в Kafka.
     *
     * <p>Процесс создания счета включает следующие шаги:</p>
     * <ol>
     *   <li>Создание объекта {@link Invoice} из данных запроса</li>
     *   <li>Установка начального статуса "NEW"</li>
     *   <li>Проставление текущей даты и времени создания</li>
     *   <li>Сохранение счета в базе данных через {@link InvoiceRepository}</li>
     *   <li>Логирование успешного сохранения с ID счета</li>
     *   <li>Отправка созданного счета в Kafka топик {@value #TOPIC_CREATE_INVOICE}</li>
     *   <li>Логирование отправки сообщения</li>
     *   <li>Возврат созданного счета клиенту</li>
     * </ol>
     *
     * <p>Логирование:</p>
     * <ul>
     *   <li>Логируется факт сохранения счета с его ID</li>
     *   <li>Логируется факт отправки сообщения в Kafka</li>
     *   <li>В случае ошибок логирование выполняется через
     *       механизмы обработки исключений Spring</li>
     * </ul>
     *
     * @param request объект запроса с данными для создания счета.
     *                Должен содержать номер аккаунта и сумму.
     *                Не может быть {@code null}.
     * @return созданная сущность {@link Invoice} с заполненными полями:
     *         <ul>
     *           <li>id — сгенерированный идентификатор</li>
     *           <li>accountNumber — из запроса</li>
     *           <li>amount — из запроса</li>
     *           <li>status — "NEW"</li>
     *           <li>createdAt — текущая дата и время</li>
     *         </ul>
     *
     * @throws IllegalArgumentException если request содержит некорректные данные
     * @throws RuntimeException если произошла ошибка при сохранении в БД
     * @throws org.springframework.kafka.KafkaException если произошла ошибка
     *         при отправке сообщения в Kafka
     * @see Transactional
     * @see InvoiceRepository#save(Object)
     * @see KafkaTemplate#send(String, Object)
     */
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