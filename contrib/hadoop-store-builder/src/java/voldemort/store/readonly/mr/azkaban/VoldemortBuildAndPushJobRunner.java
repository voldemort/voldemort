package voldemort.store.readonly.mr.azkaban;

import azkaban.utils.Props;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * A simple script to launch BnP without Azkaban.
 */
public class VoldemortBuildAndPushJobRunner {
    private static final Logger logger = Logger.getLogger(VoldemortBuildAndPushJobRunner.class);
    public static void main(String[] args) throws Exception {
        // Validate arguments
        if (args.length < 1) {
            logger.error("Please provide a job config file name as the argument to this script.");
            System.exit(1);
        }
        String fileName = args[0];

        // Load config
        logger.info("Extracting config properties out of: " + fileName);
        Props props = null;
        try {
            props = new Props(null, fileName);
        } catch (IOException e) {
            logger.error("Exception while reading config file!", e);
            System.exit(1);
        }

        // Run job
        VoldemortBuildAndPushJob job = new VoldemortBuildAndPushJob("shell-job", props);
        try {
            job.run();
        } catch (Exception e) {
            logger.error("Exception while running BnP job!", e);
            System.exit(1);
        }
        logger.info("BnP job finished successfully (:");
        System.exit(0);
    }
}
