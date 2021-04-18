package finder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FileFinder {
    private static final Logger logger = LoggerFactory.getLogger(FileFinder.class);

    public List<String> getListOfAllFiles(String directoryPath, String fileMask) {
        try {
            return Files.walk(Paths.get(directoryPath))
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toFile().getName().matches(fileMask))
                    .map(Path::toString).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Deprecated
    public List<String> getListOfFiles(String directoryName, int depth, String fileMask) {
        try {
            return Files.find(Paths.get(directoryName), depth,
                    ((path, basicFileAttributes) -> path.toFile().getName().matches(fileMask))
            ).map(Path::toString).collect(Collectors.toList());
        } catch (IOException e) {
            logger.error("Failed during reading directory", e);
            return Collections.emptyList();
        }
    }
}
