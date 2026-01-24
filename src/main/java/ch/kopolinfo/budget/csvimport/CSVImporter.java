package ch.kopolinfo.budget.csvimport;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import ch.kopolinfo.budget.db.AppDataContext;

public class CSVImporter {
    private static final String DB_URL = "jdbc:h2:file:N:/Privat/Investitionen/Budget/budget;AUTO_SERVER=TRUE";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    private final AppDataContext context;

    public CSVImporter() {
        this.context = new AppDataContext();
    }

    public void initialize() {
        try {
            System.setProperty("org.jooq.no-logo", "true");
            System.out.println("Initialisiere Anwendungskontext...");
            
            context.load(DB_URL, DB_USER, DB_PASSWORD);
            
            System.out.println("Initialisierung abgeschlossen.");
        } catch (Exception e) {
            System.err.println("Fehler bei der Initialisierung der Stammdaten!");
            e.printStackTrace();
        }
    }

    /**
     * Führt den Import-Prozess für eine Datei und einen Account aus.
     */
    public void processImport(String accountId, String filePathStr) {
        try {
            System.out.println("Starte Import für Account: " + accountId);
            
            // 1. Pfad validieren
            Path path = Paths.get(filePathStr);
            if (!path.toFile().exists()) {
                throw new IllegalArgumentException("Datei nicht gefunden: " + filePathStr);
            }

            // 2. Den richtigen Importer über die Factory im Context holen
            FileImporter fileImporter = context.getImporter(accountId);
            
            // 3. Datei parsen (Daten werden geladen, aber noch nicht verarbeitet)
            System.out.println("Parse Datei: " + path.getFileName() + " mit " + fileImporter.getClass().getSimpleName());
            List<CsvRow> rows = fileImporter.parseFile(path);

            if (rows != null) {
                System.out.println("Erfolgreich " + rows.size() + " Zeilen eingelesen.");
                // TODO: Hier folgt später der Aufruf an den ImporterService (DB Persistenz)
            } else {
                System.out.println("Der Importer lieferte keine Daten zurück.");
            }

        } catch (Exception e) {
            System.err.println("Fehler während des Import-Vorgangs:");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: CSVImporter <account_id> <file_path>");
            return;
        }

        String accountId = args[0];
        String filePath = args[1];

        CSVImporter importer = new CSVImporter();
        importer.initialize();
        
        // Führt den Import-Vorgang aus (Parsing-Stufe)
        importer.processImport(accountId, filePath);
    }

    public AppDataContext getContext() {
        return context;
    }
}
