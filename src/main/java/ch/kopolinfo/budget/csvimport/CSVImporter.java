package ch.kopolinfo.budget.csvimport;

import static ch.kopolinfo.budget.model.jooq.Tables.IMPORT_LOG;
import static ch.kopolinfo.budget.model.jooq.Tables.IMPORT_ENTRY;
import static ch.kopolinfo.budget.model.jooq.Tables.PAYMENT;
import static ch.kopolinfo.budget.model.jooq.Tables.TRANSACTION;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import ch.kopolinfo.budget.db.AppDataContext;
import ch.kopolinfo.budget.model.jooq.tables.pojos.Payment;
import ch.kopolinfo.budget.model.jooq.tables.records.ImportEntryRecord;
import ch.kopolinfo.budget.model.jooq.tables.records.ImportLogRecord;
import ch.kopolinfo.budget.model.jooq.tables.records.PaymentRecord;
import ch.kopolinfo.budget.model.jooq.tables.records.TransactionRecord;
import ch.kopolinfo.budget.rules.Rule;
import ch.kopolinfo.budget.rules.RuleFactory;

public class CSVImporter {
    private final AppDataContext context;

    public CSVImporter(AppDataContext context) {
    	this.context = context;
    }

    /**
     * Führt den Import-Prozess für eine Datei und einen Account aus.
     */
    private void processImport(String accountId, String filePathStr) {
        try {
            System.out.println("Starte Import für Account: " + accountId);
            
            Path path = Paths.get(filePathStr);
            if (!path.toFile().exists()) {
                throw new IllegalArgumentException("Datei nicht gefunden: " + filePathStr);
            }

            FileImporter fileImporter = context.getImporter(accountId);
            
            System.out.println("Parse Datei: " + path.getFileName() + " mit " + fileImporter.getClass().getSimpleName());
            List<CsvRow> rows = fileImporter.parseFile(path);

            validateImportDate(accountId, rows);
            
            importPayments(path.getFileName().toString(), accountId, rows);
        } catch (Exception e) {
            System.err.println("Fehler während des Import-Vorgangs:");
            e.printStackTrace();
        }
    }

    private void validateImportDate(String accountId, List<CsvRow> rows) {
        if (rows.isEmpty()) return;

        // 1. Datum der ersten Buchung in der CSV (Annahme: sortiert)
        LocalDate firstCsvDate = rows.get(0).bookingDate();

        // 2. Letztes Buchungsdatum in der DB für dieses Konto
        LocalDate lastDbDate = context.getLastBookingDate(accountId);

        // 3. Fail-Fast Check
        if (lastDbDate != null && !firstCsvDate.isAfter(lastDbDate)) {
            throw new IllegalStateException(
                String.format("Import abgebrochen: Erste Buchung am %s überschneidet sich mit bestehenden Daten (Letzte Buchung: %s).", 
                firstCsvDate, lastDbDate)
            );
        }
    }
    
    private void importPayments(String fileName, String accountId, List<CsvRow> csvRows) {
        // Die Rule-Engine für diesen Import-Lauf initialisieren
        Rule ruleSet = RuleFactory.getRuleSet();
        
        // Alles in einer atomaren Transaktion
        context.getDsl().transaction(configuration -> {
            DSLContext txDsl = DSL.using(configuration);

            ImportLogRecord importLog = txDsl.newRecord(IMPORT_LOG);
            importLog.setAccountId(accountId);
            importLog.setImportDate(LocalDateTime.now());
            importLog.setFileName(fileName);
            importLog.insert();
            
            Integer currentImportId = importLog.getImportId();
            
            for (CsvRow row : csvRows) {
                // 1. PaymentRecord erstellen und persistieren
                // (ID wird durch das insert() automatisch im Record aktualisiert)
                PaymentRecord paymentRec = txDsl.newRecord(PAYMENT);
                paymentRec.setAccountId(accountId);
                paymentRec.setBookingDate(row.bookingDate());
                paymentRec.setAmount(row.amount());
                paymentRec.setDescription(row.description());
                paymentRec.setRawCsvLine(row.rawLine());
                paymentRec.insert(); 

                // 2. Rule Engine anwenden
                // Wir konvertieren den Record kurz in ein POJO für das Interface
                Payment paymentPojo = paymentRec.into(Payment.class);
                String categoryId = ruleSet.categoryFor(paymentPojo)
                                           .orElse(context.getUnassignedCategory().getId());

                // 3. TransactionRecord erstellen (die Verknüpfung)
                TransactionRecord transRec = txDsl.newRecord(TRANSACTION);
                transRec.setPaymentId(paymentRec.getPaymentId());
                transRec.setCategoryId(categoryId);
                transRec.setAmount(paymentRec.getAmount());
                transRec.setValidFrom(paymentRec.getBookingDate());
                transRec.setValidTo(paymentRec.getBookingDate());
                transRec.setDescription(paymentRec.getDescription());
                
                transRec.insert();

                ImportEntryRecord entryRec = txDsl.newRecord(IMPORT_ENTRY);
                entryRec.setImportId(currentImportId);
                entryRec.setPaymentId(paymentRec.getPaymentId());
                entryRec.insert();
            }
            
            System.out.println(csvRows.size() + " Einträge erfolgreich verarbeitet.");
        });
    }    
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: CSVImporter <account_id> <file_path>");
            return;
        }

        // try-with-resources sorgt für automatisches Schließen
        try (AppDataContext context = new AppDataContext()) {
        	String accountId = args[0];
	        String filePath = args[1];
	
	        CSVImporter importer = new CSVImporter(context);
	        
	        importer.processImport(accountId, filePath);
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public AppDataContext getContext() {
        return context;
    }
}
