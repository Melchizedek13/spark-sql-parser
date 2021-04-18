package utils;

public class SQLUtils {
    public static String normalizeQuery(String query) {
        return query
                .replaceAll("\\{.?year+ *(-|\\+)? *\\d* *}", "2020")
                .replaceAll("\\{.?month+ *(-|\\+)? *\\d* *}", "01")
                .replaceAll("\\{.?day+ *(-|\\+)? *\\d* *}", "01")
                .replaceAll("\\{.?interval+ *(-|\\+)? *\\d* *}", "1");
    }
}
