package producer1.entity;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

/**
 * Сущность, представляющая счет в системе биллинга.
 *
 * <p>Данный класс является JPA-сущностью и отображается на таблицу {@code invoices}
 * в базе данных. Содержит всю необходимую информацию о финансовом документе,
 * включая данные клиента, сумму и текущий статус.</p>
 *
 * <p>Счет проходит следующий жизненный цикл:</p>
 * <ul>
 *   <li><b>NEW</b> — создан, ожидает обработки</li>
 *   <li><b>REGISTERED</b> — обработан и зарегистрирован в системе</li>
 * </ul>
 *
 * <p>При создании счета автоматически генерируется идентификатор и
 * проставляется дата создания. Статус по умолчанию устанавливается в "NEW".</p>
 *
 * @author Andrus Gregory
 * @see producer1.service.InvoiceService
 * @see producer1.controller.InvoiceController
 */
@Entity
@Table(name = "invoices")
@Data
public class Invoice {
    /**
     * Уникальный идентификатор счета.
     *
     * <p>Генерируется автоматически базой данных при сохранении новой записи.
     * Используется в качестве первичного ключа таблицы {@code invoices}.</p>
     *
     * <p>Стратегия генерации: {@link GenerationType#IDENTITY} — автоинкремент
     * в базе данных, что обеспечивает уникальность и последовательность
     * идентификаторов.</p>
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Номер счета.
     */
    private String accountNumber;

    /**
     * Сумма счета в базовой валюте системы.
     *
     * <p>Финансовая сумма, подлежащая оплате клиентом.
     * Всегда положительное значение.</p>
     *
     */
    private Double amount;

    /**
     * Текущий статус счета в системе.
     *
     * <p>Определяет этап жизненного цикла счета.</p>
     *
     * <p>Возможные значения:</p>
     * <ul>
     *   <li><b>NEW</b> — счет только что создан, ожидает обработки</li>
     *   <li><b>REGISTERED</b> — счет обработан и зарегистрирован</li>
     * </ul>
     *
     * <p>При создании счета статус автоматически устанавливается в "NEW".</p>
     */
    private String status; // NEW, REGISTERED

    /**
     * Дата и время создания счета.
     *
     * <p>Автоматически проставляется при сохранении сущности в базу данных.
     * Хранится в колонке {@code created_at} таблицы {@code invoices}.</p>
     *
     * <p>Время сохраняется в UTC и используется для:</p>
     * <ul>
     *   <li>Отслеживания времени создания документа</li>
     *   <li>Аудита и логирования операций</li>
     *   <li>Расчета сроков оплаты</li>
     * </ul>
     * @see org.springframework.data.annotation.CreatedDate
     */
    @Column(name = "created_at")
    private LocalDateTime createdAt;
}