package utils;

import java.util.Arrays;
import java.util.List;

public class FileUtils {

    public static String makeFileName(
            String fileName,
            String splitDelimiter,
            int startSlicingPosition,
            String linkDelimiter
    ) {
        List<String> fileItems = Arrays.asList(fileName.split(splitDelimiter));
        fileItems = fileItems.subList(fileItems.size() - startSlicingPosition, fileItems.size());
        return String.join(linkDelimiter, fileItems);
    }

}
