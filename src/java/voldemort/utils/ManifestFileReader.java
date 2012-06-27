package voldemort.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ManifestFileReader {

    protected static final Logger logger = Logger.getLogger(ManifestFileReader.class);

    private static String MANIFEST_FILE = "META-INF/MANIFEST.MF";
    private static String RELEASE_VERSION_KEY = "Implementation-Version";

    public static String getReleaseVersion() {
        String version = null;
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(MANIFEST_FILE));
            version = properties.getProperty(RELEASE_VERSION_KEY);
        } catch(IOException e) {
            logger.warn("Unable to load voldemort release version due to the following error:", e);
        }
        return version;
    }
}
