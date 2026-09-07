package consumer1.service;

import consumer1.dto.InvoiceMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Сервис-потребитель сообщений Kafka для обработки счетов (инвойсов).
 *
 * <p>Основная задача сервиса — принимать сообщения из топика {@value #TOPIC_CREATED},
 * обрабатывать их (симулировать бизнес-логику) и отправлять обработанные
 * сообщения в топик {@value #TOPIC_REGISTERED} для дальнейшего использования
 * другими микросервисами.</p>
 *
 * <p>Сервис реализует паттерн "Competing Consumers" и может быть масштабирован
 * горизонтально за счет увеличения количества экземпляров приложения,
 * работающих с одной группой потребителей {@code consumer-group-1}.</p>
 *
 * @author Andrus Gregory
 * @see InvoiceMessage
 * @see KafkaTemplate
 * @see KafkaListener
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceConsumerService {

    /**
     * Шаблон для отправки сообщений в Kafka.
     * Используется для публикации обработанных сообщений в выходной топик.
     *
     * <p>Типизирован как {@code KafkaTemplate<String, Object>}, где:
     * <ul>
     *   <li>String — тип ключа сообщения (обычно ID или null)</li>
     *   <li>Object — тип значения (в данном случае {@link InvoiceMessage})</li>
     * </ul>
     * </p>
     */
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Топик входящих сообщений со статусом "CREATED".
     * В этот топик продюсер отправляет новые счета,
     * требующие первичной обработки и регистрации.
     */
    private static final String TOPIC_CREATED = "invoice-created";

    /**
     * Топик исходящих сообщений со статусом "REGISTERED".
     * После обработки счет отправляется в этот топик
     * для последующей обработки другими сервисами (например,
     * для отправки уведомлений или сохранения в базе данных).
     */
    private static final String TOPIC_REGISTERED = "invoice-registered";

    /**
     * Обрабатывает входящие сообщения из топика {@value #TOPIC_CREATED}.
     *
     * <p>Метод автоматически вызывается фреймворком Spring при поступлении
     * нового сообщения в топик. Аннотация {@link KafkaListener} обеспечивает
     * подписку на указанный топик с заданной группой потребителей.</p>
     *
     * <p>Процесс обработки включает следующие шаги:</p>
     * <ol>
     *   <li>Логирование полученного сообщения для аудита и отладки</li>
     *   <li>Имитация бизнес-логики (задержка в 1 секунду для эмуляции работы)</li>
     *   <li>Изменение статуса счета-фактуры на "REGISTERED"</li>
     *   <li>Отправка обновленного сообщения в выходной топик {@value #TOPIC_REGISTERED}</li>
     * </ol>
     *
     * <p>Важные особенности:</p>
     * <ul>
     *   <li>При возникновении {@link InterruptedException} происходит корректное
     *       восстановление прерванного состояния потока</li>
     *   <li>Все операции логируются с использованием SLF4J</li>
     *   <li>Метод не обрабатывает исключения явно — при ошибках механизм
     *       Kafka обеспечивает повторную доставку сообщений</li>
     * </ul>
     *
     * @param message объект счета-фактуры, полученный из Kafka.
     *                Должен содержать заполненные поля id, accountNumber,
     *                amount, status (ожидается "CREATED") и createdAt.
     *
     * @throws InterruptedException если поток был прерван во время имитации обработки
     * @see InvoiceMessage
     * @see KafkaTemplate
     * @see KafkaListener
     */
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