package writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class FileWriter {
    private static final Logger logger = LoggerFactory.getLogger(FileWriter.class);

    public void writeToFile(String dirPath, String fileName, List<String> tableNamesList) {
        try {
            Files.write(Paths.get(dirPath, fileName), tableNamesList, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            logger.error("An error occurred while writing data to the file!", e);
        }

    }

}
