package ch.kopolinfo.budget.csvimport;

import static ch.kopolinfo.budget.model.jooq.Tables.IMPORT_ENTRY;
import static ch.kopolinfo.budget.model.jooq.Tables.IMPORT_LOG;
import static ch.kopolinfo.budget.model.jooq.Tables.PAYMENT;
import static ch.kopolinfo.budget.model.jooq.Tables.TRANSACTION;

import java.util.List;

import org.jooq.DSLContext;

import ch.kopolinfo.budget.db.AppDataContext;

public class FileDeleter {

    private static final String DB_URL = "jdbc:h2:file:N:/Privat/Investitionen/Budget/budget;AUTO_SERVER=TRUE";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: FileDeleter <import_log_id>");
            return;
        }

        int importId = Integer.parseInt(args[0]);

        // Nutzung von try-with-resources für den AppDataContext (AutoCloseable)
        try (AppDataContext context = new AppDataContext(DB_URL, DB_USER, DB_PASSWORD)) {
            DSLContext dsl = context.getDsl();

            System.out.println("Starte Löschvorgang für Import ID: " + importId);

            // Alles in einer Transaktion, um Teil-Löschungen bei Fehlern zu vermeiden
            dsl.transaction(configuration -> {
                var txDsl = configuration.dsl();

                // 1. Alle Payment-IDs für diesen Import sammeln
                List<Integer> paymentIds = txDsl.select(IMPORT_ENTRY.PAYMENT_ID)
                        .from(IMPORT_ENTRY)
                        .where(IMPORT_ENTRY.IMPORT_ID.eq(importId))
                        .fetchInto(Integer.class);

                if (!paymentIds.isEmpty()) {
                    // ZUERST: Transaktionen löschen, die auf diese Payments verweisen
                    int deletedTrans = txDsl.deleteFrom(TRANSACTION)
                            .where(TRANSACTION.PAYMENT_ID.in(paymentIds))
                            .execute();
                    System.out.println(deletedTrans + " zugehörige Transactions gelöscht.");

                    // DANACH: Payments löschen
                    int deletedPayments = txDsl.deleteFrom(PAYMENT)
                            .where(PAYMENT.PAYMENT_ID.in(paymentIds))
                            .execute();
                    System.out.println(deletedPayments + " Payments gelöscht.");
                }

                // ZULETZT: Import-Log löschen (IMPORT_ENTRY wird durch dein Schema-Cascade gelöscht)
                txDsl.deleteFrom(IMPORT_LOG)
                        .where(IMPORT_LOG.IMPORT_ID.eq(importId))
                        .execute();

                System.out.println("Import-Log und Verknüpfungen erfolgreich entfernt.");
            });

            System.out.println("Löschvorgang erfolgreich abgeschlossen.");

        } catch (Exception e) {
            System.err.println("Fehler beim Löschen des Imports:");
            e.printStackTrace();
        }
    }
}
