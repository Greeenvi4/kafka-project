package producer1.controller;

import producer1.dto.CreateInvoiceRequest;
import producer1.entity.Invoice;
import producer1.service.InvoiceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST-контроллер для управления счетами (инвойсами).
 *
 * <p>Предоставляет HTTP API для создания новых счетов в системе.
 * Является точкой входа для внешних клиентов (веб-интерфейс, другие сервисы,
 * интеграционные системы), инициирующих процесс создания счета.</p>
 *
 * <p>Контроллер работает в связке с {@link InvoiceService}, который содержит
 * основную бизнес-логику, и является частью архитектуры Producer Service,
 * отвечающей за генерацию и отправку сообщений в Kafka.</p>
 *
 *
 * <p>Все эндпоинты контроллера доступны по базовому пути {@code /api/invoices}
 * и возвращают ответы в формате JSON.</p>
 * @see InvoiceService
 * @see CreateInvoiceRequest
 * @see Invoice
 * @author Andrus Gregory
 */
@RestController
@RequestMapping("/api/invoices")
@RequiredArgsConstructor
public class InvoiceController {

    /**
     * Сервис для работы со счетами-фактурами.
     * Содержит бизнес-логику создания счета и отправки сообщения в Kafka.
     * Внедряется через конструктор благодаря аннотации {@link RequiredArgsConstructor}.
     */
    private final InvoiceService invoiceService;

    /**
     * Создает новый счет-фактуру на основе данных запроса.
     *
     * <p>Эндпоинт: {@code POST /api/invoices}</p>
     *
     * <p>Процесс обработки запроса включает следующие шаги:</p>
     * <ol>
     *   <li>Прием DTO {@link CreateInvoiceRequest} с данными для создания счета</li>
     *   <li>Валидация входных данных (выполняется автоматически через аннотации
     *       валидации в DTO, если они присутствуют)</li>
     *   <li>Передача запроса в {@link InvoiceService} для выполнения бизнес-логики</li>
     *   <li>Сохранение счета в базе данных (внутри сервиса)</li>
     *   <li>Отправка сообщения в Kafka топик {@code invoice-created}</li>
     *   <li>Возврат созданной сущности {@link Invoice} клиенту</li>
     * </ol>
     *
     * @param request объект запроса с данными для создания счета.
     *                Должен содержать обязательные поля для создания счета
     *                (номер счета, сумма и т.д.)
     * @return {@link ResponseEntity} с созданной сущностью {@link Invoice} в теле ответа.
     *         Статус ответа — 200 OK.
     *
     * @throws IllegalArgumentException если входные данные не прошли валидацию
     * @throws RuntimeException если произошла ошибка при создании счета или
     *                          при отправке сообщения в Kafka
     * @see PostMapping
     * @see RequestBody
     * @see CreateInvoiceRequest
     * @see Invoice
     */
    @PostMapping
    public ResponseEntity<Invoice> createInvoice(@RequestBody CreateInvoiceRequest request) {
        Invoice invoice = invoiceService.createInvoice(request);
        return ResponseEntity.ok(invoice);
    }
}