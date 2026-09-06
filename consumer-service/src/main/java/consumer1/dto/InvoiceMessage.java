package consumer1.dto;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class InvoiceMessage {
    private Long id;
    private String accountNumber;
    private Double amount;
    private String status;
    private LocalDateTime createdAt;
}