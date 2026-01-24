package ch.kopolinfo.budget.csvimport.importer;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import ch.kopolinfo.budget.csvimport.CsvRow;
import ch.kopolinfo.budget.csvimport.FileImporter;

public class RaiffeisenImporter implements FileImporter {

    // Format: 2026-01-05 00:00:00.0
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");

    @Override
    public List<CsvRow> parseFile(Path filePath) throws Exception {
        List<CsvRow> rows = new ArrayList<>();
        
        // Alle Zeilen lesen
        List<String> lines = Files.readAllLines(filePath);
        
        // Header überspringen (IBAN;Booked At...)
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line == null || line.isBlank()) continue;
            
            String[] columns = line.split(";");
            
            if (columns.length >= 4) {
                // 1. Datum parsen
                LocalDateTime dateTime = LocalDateTime.parse(columns[1], DATE_FORMATTER);
                
                // 2. Text extrahieren (Partner/Zweck Mischmasch)
                String text = columns[2];
                
                // 3. Betrag
                BigDecimal amount = new BigDecimal(columns[3]);
                
                // CsvRow erstellen
                rows.add(new CsvRow(
                    dateTime.toLocalDate(), // bookingDate
                    amount,                 // amount
                    text,                   // partnerName (initial identisch mit Text)
                    text,                   // purpose (initial identisch mit Text)
                    line                    // rawLine
                ));
            }
        }
        
        return rows;
    }
}
