package ch.kopolinfo.budget.rules;

import java.util.List;
import java.util.Optional;

import ch.kopolinfo.budget.model.jooq.tables.pojos.Payment;

public class RuleSequence implements Rule {
	final List<Rule> rules;
	
	public RuleSequence(List<Rule> rules) {
		this.rules = rules;
	}
	
	@Override
	public Optional<String> categoryFor(Payment payment) {
	    return rules.stream()
	            .map(rule -> rule.categoryFor(payment))
	            .flatMap(Optional::stream)
	            .findFirst();
	}
}
