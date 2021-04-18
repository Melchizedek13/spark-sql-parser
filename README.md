# spark-sql-parse
Parsing sql queries to get table names by using the CatalystSqlParser.

The project compares the correctness of the extraction of table names between the naive Spark-parser and the Python regular expression.

The `src/main/java/Main.java` application is launched first. Then the `regexp_check_table_names.py` module is run.

The following environment variables need to be set:  
- `DIR_PATH_WITH_SQL_FILES`=  
- `REGEXP_DIR_PATH`=  
- `SPARK_DIR_PATH`=

