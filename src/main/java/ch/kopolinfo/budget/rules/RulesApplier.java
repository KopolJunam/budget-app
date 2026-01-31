package ch.kopolinfo.budget.rules;

import static ch.kopolinfo.budget.model.jooq.Tables.PAYMENT;
import static ch.kopolinfo.budget.model.jooq.Tables.TRANSACTION;

import java.util.List;
import java.util.Optional;

import org.jooq.DSLContext;

import ch.kopolinfo.budget.db.AppDataContext;
import ch.kopolinfo.budget.model.jooq.tables.pojos.Payment;
import ch.kopolinfo.budget.model.jooq.tables.records.TransactionRecord;

public class RulesApplier {

    public static void main(String[] args) {
        System.setProperty("org.jooq.no-logo", "true");

        try (AppDataContext context = new AppDataContext()) {
            DSLContext dsl = context.getDsl();
            Rule ruleSet = RuleFactory.getRuleSet();
            String unassignedId = context.getUnassignedCategory().getId();

            System.out.println("Suche nach unkategorisierten Transaktionen...");

            // 1. Hole alle TransactionRecords, die UNASSIGNED sind
            // Wir joinen das Payment, damit wir die Daten für die Rule-Engine haben
            List<TransactionRecord> unassignedTransactions = dsl.select(TRANSACTION.fields())
                    .from(TRANSACTION)
                    .where(TRANSACTION.CATEGORY_ID.eq(unassignedId))
                    .fetchInto(TRANSACTION);

            if (unassignedTransactions.isEmpty()) {
                System.out.println("Keine unkategorisierten Transaktionen gefunden.");
                return;
            }

            System.out.println(unassignedTransactions.size() + " Transaktionen gefunden. Wende Regeln an...");

            dsl.transaction(configuration -> {
                int updateCount = 0;
                for (TransactionRecord trans : unassignedTransactions) {
                    
                    // 2. Passendes Payment laden, um es der Rule Engine zu übergeben
                    Payment payment = dsl.selectFrom(PAYMENT)
                            .where(PAYMENT.PAYMENT_ID.eq(trans.getPaymentId()))
                            .fetchOneInto(Payment.class);

                    if (payment != null) {
                        // 3. Regeln prüfen
                        Optional<String> newCategory = ruleSet.categoryFor(payment);
                        
                        if (newCategory.isPresent()) {
                            // 4. Update durchführen, falls eine Regel matcht
                            trans.setCategoryId(newCategory.get());
                            trans.store(); // Speichert die Änderung in der DB
                            updateCount++;
                        }
                    }
                }
                System.out.println("Update abgeschlossen: " + updateCount + " Transaktionen neu kategorisiert.");
            });

        } catch (Exception e) {
            System.err.println("Fehler beim Anwenden der Regeln:");
            e.printStackTrace();
        }
    }
}
