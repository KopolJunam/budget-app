package ch.kopolinfo.budget.csvimport;

import java.math.BigDecimal;
import java.time.LocalDate;

public record CsvRow(
	    LocalDate bookingDate,
	    BigDecimal amount,
	    String partnerName,
	    String purpose,
	    String rawLine
	) {}