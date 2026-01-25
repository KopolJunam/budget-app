package ch.kopolinfo.budget.rules;

public class RuleFactory {
	public static Rule getRuleSet() {
		return new PartnerMatch();
	}
}
