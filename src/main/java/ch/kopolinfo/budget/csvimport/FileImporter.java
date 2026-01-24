package ch.kopolinfo.budget.csvimport;

import java.nio.file.Path;
import java.util.List;

public interface FileImporter {
    List<CsvRow> parseFile(Path filePath) throws Exception;
}
