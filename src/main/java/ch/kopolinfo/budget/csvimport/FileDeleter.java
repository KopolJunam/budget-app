package ch.kopolinfo.budget.csvimport;

import static ch.kopolinfo.budget.model.jooq.Tables.IMPORT_ENTRY;
import static ch.kopolinfo.budget.model.jooq.Tables.IMPORT_LOG;
import static ch.kopolinfo.budget.model.jooq.Tables.PAYMENT;
import static ch.kopolinfo.budget.model.jooq.Tables.TRANSACTION;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.jooq.DSLContext;
import ch.kopolinfo.budget.db.AppDataContext;

public class FileDeleter {

    public static void main(String[] args) {
        // Erwartet nun: <HH:MM> <import_log_id>
        if (args.length < 2) {
            System.out.println("Usage: FileDeleter <HH:MM> <import_log_id>");
            return;
        }

        String safetyTimeStr = args[0];
        int importId = Integer.parseInt(args[1]);

        // Sicherheitsprüfung der Zeit
        if (!isTimeValid(safetyTimeStr)) {
            System.err.println("Sicherheitscheck fehlgeschlagen: Die angegebene Zeit " + safetyTimeStr + 
                               " weicht zu stark von der aktuellen Zeit ab oder ist ungültig.");
            return;
        }

        try (AppDataContext context = new AppDataContext()) {
            DSLContext dsl = context.getDsl();
            System.out.println("Sicherheitscheck OK. Starte Löschvorgang für Import ID: " + importId);

            dsl.transaction(configuration -> {
                var txDsl = configuration.dsl();

                List<Integer> paymentIds = txDsl.select(IMPORT_ENTRY.PAYMENT_ID)
                        .from(IMPORT_ENTRY)
                        .where(IMPORT_ENTRY.IMPORT_ID.eq(importId))
                        .fetchInto(Integer.class);

                if (!paymentIds.isEmpty()) {
                    int deletedTrans = txDsl.deleteFrom(TRANSACTION)
                            .where(TRANSACTION.PAYMENT_ID.in(paymentIds))
                            .execute();
                    System.out.println(deletedTrans + " zugehörige Transactions gelöscht.");

                    int deletedPayments = txDsl.deleteFrom(PAYMENT)
                            .where(PAYMENT.PAYMENT_ID.in(paymentIds))
                            .execute();
                    System.out.println(deletedPayments + " Payments gelöscht.");
                }

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

    private static boolean isTimeValid(String inputTimeStr) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm");
            LocalTime inputTime = LocalTime.parse(inputTimeStr, formatter);
            // Wir ignorieren Sekunden für den Vergleich
            LocalTime now = LocalTime.now().truncatedTo(ChronoUnit.MINUTES);
            
            // Erlaubt: Eingabe entspricht jetzt ODER Eingabe ist genau 1 Minute vor jetzt
            // (Das deckt deinen Wunsch ab: Wenn 17:25 übergeben wird, darf es 17:25 oder 17:26 sein)
            return inputTime.equals(now) || inputTime.plusMinutes(1).equals(now);
        } catch (Exception e) {
            return false;
        }
    }
}
