package parser;

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class SQLParser {
    private static final Logger logger = LoggerFactory.getLogger(SQLParser.class);

    public Set<String> getTableNames(final String query) {
        final CatalystSqlParser catalystSqlParser = new CatalystSqlParser();

        Set<String> tableNames = new HashSet<>();
        try {
            LogicalPlan logicalPlan = catalystSqlParser.parsePlan(query);
            int i = 0;

            while (true) {
                if (logicalPlan.apply(i) == null) {
                    return tableNames;
                } else if (logicalPlan.apply(i) instanceof UnresolvedRelation) {
                    if (((UnresolvedRelation) logicalPlan.apply(i)).multipartIdentifier().length() != 1) {
                        tableNames.add(((UnresolvedRelation) logicalPlan.apply(i)).tableName());
                    }
                }
                i++;
            }
        } catch (Exception e) {
            logger.error("Failed to parse query", e);
            return Collections.emptySet();
        }
    }

}
