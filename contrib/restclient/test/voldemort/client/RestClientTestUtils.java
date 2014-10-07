package voldemort.client;

import java.io.File;

import org.apache.commons.io.FileUtils;

public class RestClientTestUtils {

    public static void copyFile(String sourceFilePath, String destinationFilePath) throws Exception {
        File src = new File(sourceFilePath);
        File dest = new File(destinationFilePath);
        FileUtils.copyFile(src, dest);
    }

}
