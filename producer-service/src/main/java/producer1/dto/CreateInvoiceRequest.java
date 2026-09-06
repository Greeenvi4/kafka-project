package producer1.dto;

import lombok.Data;

@Data
public class CreateInvoiceRequest {
    private String accountNumber;
    private Double amount;
}