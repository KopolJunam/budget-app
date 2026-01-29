package ch.kopolinfo.budget.csvimport.importer;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Stream;

import ch.kopolinfo.budget.csvimport.CsvRow;
import ch.kopolinfo.budget.csvimport.FileImporter;

public class CembraImporter implements FileImporter {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy");

    @Override
    public List<CsvRow> parseFile(Path filePath) throws Exception {
        try (Stream<String> lines = Files.lines(filePath)) {
            List<CsvRow> rows = lines
                .skip(1) // Header überspringen
                .filter(line -> !line.isBlank())
                .map(this::mapToCsvRow)
                .toList();

            // Da Cembra absteigend liefert, für die Watermark-Prüfung umkehren
            return rows.reversed();
        }
    }

    private CsvRow mapToCsvRow(String line) {
        String[] columns = line.split(",");
        
        // Index-Mapping basierend auf Cembra-Format:
        // 2: Booking date, 3: Merchant, 4: Description, 5: Type, 6: Amount
        LocalDate bookingDate = LocalDate.parse(columns[2].trim(), DATE_FORMATTER);
        
        String merchant = columns[3].trim();
        String detail = columns[4].trim();
        String description = detail.startsWith(merchant) ? merchant : merchant + " (" + detail + ")";
        
        BigDecimal amount = new BigDecimal(columns[6].trim());
        String type = columns[5].trim();
        
        // Logik: Debit (Ausgabe) wird negativ, Credit (Gutschrift/Zahlung) positiv
        if ("Debit".equalsIgnoreCase(type)) {
            amount = amount.negate();
        }

        return new CsvRow(
            bookingDate,
            amount,
            description,
            line
        );
    }
}
