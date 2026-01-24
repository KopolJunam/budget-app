package ch.kopolinfo.budget.rules;

import java.util.Optional;

import ch.kopolinfo.budget.model.jooq.tables.pojos.Payment;

public interface Rule {
	Optional<String> categoryFor(Payment payment);
}
