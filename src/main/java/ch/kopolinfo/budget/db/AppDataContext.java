package ch.kopolinfo.budget.db;

import static ch.kopolinfo.budget.model.jooq.Tables.ACCOUNT;
import static ch.kopolinfo.budget.model.jooq.Tables.CATEGORY;
import static ch.kopolinfo.budget.model.jooq.Tables.CATEGORY_GROUP;
import static ch.kopolinfo.budget.model.jooq.Tables.CURRENCY;
import static ch.kopolinfo.budget.model.jooq.Tables.PAYMENT;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
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

public class AppDataContext implements AutoCloseable {
    private final Connection connection;
    private final DSLContext dsl;

    private Map<String, Account> accounts;
    private Map<String, Category> categories;
    private Map<String, CategoryGroup> categoryGroups;
    private Map<String, Currency> currencies;

    public AppDataContext(String url, String user, String password) throws Exception {
        // Initialisierung der finalen Member
        this.connection = DriverManager.getConnection(url, user, password);
        this.dsl = DSL.using(connection);
        
        // jOOQ-Logo unterdrücken (optional, falls gewünscht)
        System.setProperty("org.jooq.no-logo", "true");

        loadMetadata();
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }
    
    public DSLContext getDsl() {
        return dsl;
    }
    
	private void loadMetadata() {
        currencies = dsl.selectFrom(CURRENCY).fetchInto(Currency.class)
                .stream().collect(Collectors.toMap(Currency::getCurrencyCode, c -> c));

        accounts = dsl.selectFrom(ACCOUNT).fetchInto(Account.class)
                .stream().collect(Collectors.toMap(Account::getId, a -> a));

        categoryGroups = dsl.selectFrom(CATEGORY_GROUP).fetchInto(CategoryGroup.class)
                .stream().collect(Collectors.toMap(CategoryGroup::getId, g -> g));

        categories = dsl.selectFrom(CATEGORY).fetchInto(Category.class)
                .stream().collect(Collectors.toMap(Category::getId, c -> c));
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
    public CategoryGroup getCategoryGroup(String id) { return categoryGroups.get(id); }
    public Category getCategory(String id) { return categories.get(id); }
    public Currency getCurrency(String code) { return currencies.get(code); }
    
    // Hilfsmethode für den Importer-Service (Suche der "Nicht zugeordnet"-Kategorie)
    public Category getUnassignedCategory() {
        return categories.values().stream()
                .filter(c -> c.getId().equals("UNASSIGNED"))
                .findFirst()
                .orElse(null);
    }

    public LocalDate getLastBookingDate(String accountId) {
        // Falls die Payments nicht alle im Speicher sind, fragen wir jOOQ direkt
        return dsl.select(DSL.max(PAYMENT.BOOKING_DATE))
                  .from(PAYMENT)
                  .where(PAYMENT.ACCOUNT_ID.eq(accountId))
                  .fetchOneInto(LocalDate.class);
    }    
}
