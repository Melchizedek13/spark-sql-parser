import finder.FileFinder;
import parser.SQLParser;
import reader.FileReader;
import utils.FileUtils;
import writer.FileWriter;

import java.util.List;
import java.util.stream.Collectors;

import static utils.SQLUtils.normalizeQuery;

public class Main {
    public static void main(String[] args) {
        final String baseDir = "/Users/nikolaysokolov";
        final String dirPathWithSQLFiles = System.getProperty("DIR_PATH_WITH_SQL_FILES", baseDir + "/git/tableau_airflow/services/airflow/dags/spark-submit/sql/storage");
        final String dirSparkParsingResultPath = System.getProperty("SPARK_DIR_PATH", baseDir + "/tmp/uchi.ru/spark-parse-out");
        final String fileExtension = ".*.sql";


        final FileFinder fileFinder = new FileFinder();
        final FileWriter fileWriter = new FileWriter();
        final FileReader fileReader = new FileReader();
        final SQLParser sqlParser = new SQLParser();

        final List<String> listOfFilePaths = fileFinder.getListOfAllFiles(dirPathWithSQLFiles, fileExtension);

        for (String sqlFilePath : listOfFilePaths) {
            System.out.println(sqlFilePath);

            final String sparkFileName = FileUtils.makeFileName(sqlFilePath, "/", 3, "__");
            final String sqlText = normalizeQuery(fileReader.getFileContent(sqlFilePath));


            final List<String> tableNames = sqlParser.getTableNames(sqlText).stream().sorted().collect(Collectors.toList());
            if (tableNames.size() != 0) {
                fileWriter.writeToFile(dirSparkParsingResultPath, sparkFileName, tableNames);
            }

        }
    }
}
