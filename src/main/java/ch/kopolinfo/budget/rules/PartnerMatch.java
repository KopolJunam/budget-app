package ch.kopolinfo.budget.rules;

import java.util.Map;
import java.util.Optional;

import ch.kopolinfo.budget.model.jooq.tables.pojos.Payment;

public class PartnerMatch implements Rule {
	Map<String, String> patternToCategory = Map.of(
			"MIGROS KREUZPLATZ", "MIGROSKREUZ"
			);
			
	
	@Override
	public Optional<String> categoryFor(Payment payment) {
		return patternToCategory
		.keySet()
		.stream()
		.filter(pattern -> payment.getPartnerName().contains(pattern))
		.map(patternToCategory::get)
		.findFirst();
	}

}
