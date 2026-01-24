package ch.kopolinfo.budget.db;

import static ch.kopolinfo.budget.model.jooq.Tables.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import ch.kopolinfo.budget.csvimport.FileImporter;
import ch.kopolinfo.budget.csvimport.importer.RaiffeisenImporter;
import ch.kopolinfo.budget.model.jooq.tables.pojos.Account;
import ch.kopolinfo.budget.model.jooq.tables.pojos.Category;
import ch.kopolinfo.budget.model.jooq.tables.pojos.CategoryGroup;
import ch.kopolinfo.budget.model.jooq.tables.pojos.Currency;

public class AppDataContext {

    private Map<String, Account> accounts;
    private Map<String, Category> categories;
    private Map<String, CategoryGroup> categoryGroups;
    private Map<String, Currency> currencies;

    public void load(String url, String user, String password) throws Exception {
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            DSLContext ctx = DSL.using(conn);

            currencies = ctx.selectFrom(CURRENCY).fetchInto(Currency.class)
                    .stream().collect(Collectors.toMap(Currency::getCurrencyCode, c -> c));

            accounts = ctx.selectFrom(ACCOUNT).fetchInto(Account.class)
                    .stream().collect(Collectors.toMap(Account::getId, a -> a));

            categoryGroups = ctx.selectFrom(CATEGORY_GROUP).fetchInto(CategoryGroup.class)
                    .stream().collect(Collectors.toMap(CategoryGroup::getId, g -> g));

            categories = ctx.selectFrom(CATEGORY).fetchInto(Category.class)
                    .stream().collect(Collectors.toMap(Category::getId, c -> c));

            System.out.println("Stammdaten erfolgreich geladen: " 
                + accounts.size() + " Konten, " 
                + categories.size() + " Kategorien.");
        }
    }

    /**
     * Factory-Methode, die basierend auf der Account-ID den passenden Importer zurückgibt.
     */
    public FileImporter getImporter(String accountId) {
        if (!accounts.containsKey(accountId)) {
            throw new IllegalArgumentException("Account mit ID '" + accountId + "' existiert nicht in der Datenbank.");
        }

        // Zuordnung der IDs zu den konkreten Importer-Implementierungen
        return switch (accountId) {
            case "RAIFFEISEN_PRIVAT", "RAIFFEISEN_SPAR" -> new RaiffeisenImporter();
            // Hier können später weitere Formate hinzugefügt werden
            // case "REVOLUT_01" -> new RevolutImporter();
            default -> throw new UnsupportedOperationException("Keine Importer-Logik für Account-Typ " + accountId + " definiert.");
        };
    }

    // Getter
    public Account getAccount(String id) { return accounts.get(id); }
    public Category getCategory(String id) { return categories.get(id); }
    public Currency getCurrency(String code) { return currencies.get(code); }
    
    // Hilfsmethode für den Importer-Service (Suche der "Nicht zugeordnet"-Kategorie)
    public Category getUnassignedCategory() {
        return categories.values().stream()
                .filter(c -> c.getId().equals("UNASSIGNED"))
                .findFirst()
                .orElse(null);
    }
}
