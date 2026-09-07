package producer1.repository;

import producer1.entity.Invoice;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Репозиторий для работы с сущностью {@link Invoice} в базе данных.
 *
 * <p>Предоставляет стандартный набор методов CRUD (Create, Read, Update, Delete)
 * для управления счетами, а также возможность расширения функциональности
 * за счет пользовательских запросов.</p>
 *
 * <p>Интерфейс наследует {@link JpaRepository}, что дает готовую реализацию
 * основных операций с базой данных без необходимости написания SQL-запросов:</p>
 * <ul>
 *   <li>{@code save()} — сохранение или обновление счета</li>
 *   <li>{@code findById()} — поиск счета по идентификатору</li>
 *   <li>{@code findAll()} — получение всех счетов</li>
 *   <li>{@code deleteById()} — удаление счета по ID</li>
 *   <li>{@code existsById()} — проверка существования счета</li>
 *   <li>{@code count()} — подсчет общего количества счетов</li>
 * </ul>
 *
 * <p>При необходимости репозиторий может быть расширен пользовательскими
 * методами запросов. Примеры:</p>
 * <pre>
 * // Найти все счета по номеру аккаунта
 * List&lt;Invoice&gt; findByAccountNumber(String accountNumber);
 *
 * // Найти все счета с определенным статусом
 * List&lt;Invoice&gt; findByStatus(String status);
 *
 * // Найти счета, созданные после указанной даты
 * List&lt;Invoice&gt; findByCreatedAtAfter(LocalDateTime date);
 *
 * // Найти счета по статусу и отсортировать по дате создания
 * List&lt;Invoice&gt; findByStatusOrderByCreatedAtDesc(String status);
 * </pre>
 *
 * <p>Используется в {@link producer1.service.InvoiceService} для выполнения
 * операций с базой данных при создании и обработке счетов.</p>
 *
 * @author Andrus Gregory
 * @see Invoice
 * @see JpaRepository
 * @see producer1.service.InvoiceService
 */
public interface InvoiceRepository extends JpaRepository<Invoice, Long> {
}