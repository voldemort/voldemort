package voldemort.store.bdb.dataconversion;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import voldemort.utils.CmdUtils;

/**
 * 
 * Conversion Utility to convert to-fro between 0.96 format and release 1.x+ BDB
 * data formats
 * 
 */
public class BdbConvertData {

    static Logger logger = Logger.getLogger(BdbConvertData.class);

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("cluster-xml", "[REQUIRED] path to cluster.xml file for the server")
              .withRequiredArg()
              .describedAs("cluster-xml")
              .ofType(String.class);
        parser.accepts("src", "[REQUIRED] Source environment to be converted")
              .withRequiredArg()
              .describedAs("source-env")
              .ofType(String.class);
        parser.accepts("dest", "[REQUIRED] Destination environment to place converted data into")
              .withRequiredArg()
              .describedAs("destination-env")
              .ofType(String.class);
        parser.accepts("store", "[REQUIRED] Store/BDB database to convert")
              .withRequiredArg()
              .describedAs("store")
              .ofType(String.class);
        parser.accepts("from-format", "[REQUIRED] source format")
              .withRequiredArg()
              .describedAs("from-format")
              .ofType(String.class);
        parser.accepts("to-format", "[REQUIRED] destination format")
              .withRequiredArg()
              .describedAs("to-format")
              .ofType(String.class);
        parser.accepts("je-log-size", "[Optional] Size of the converted JE log files")
              .withRequiredArg()
              .describedAs("je-log-size")
              .ofType(Integer.class);
        parser.accepts("btree-nodemax", "[Optional] Fanout of converted Btree nodes")
              .withRequiredArg()
              .describedAs("btree-nodemax")
              .ofType(Integer.class);

        OptionSet options = parser.parse(args);

        if(!options.has("cluster-xml") || !options.has("src") || !options.has("dest")
           || !options.has("store") || !options.has("from-format") || !options.has("to-format")) {
            parser.printHelpOn(System.err);
            System.exit(0);
        }

        String clusterXmlPath = CmdUtils.valueOf(options, "cluster-xml", null);
        String sourceEnvPath = CmdUtils.valueOf(options, "src", null);
        String destEnvPath = CmdUtils.valueOf(options, "dest", null);
        String storeName = CmdUtils.valueOf(options, "store", null);

        String fromFormat = CmdUtils.valueOf(options, "from-format", null);
        String toFormat = CmdUtils.valueOf(options, "to-format", null);

        if(!isValidFormat(fromFormat) || !isValidFormat(toFormat)) {
            parser.printHelpOn(System.err);
            System.exit(0);
        }

        Integer logFileSize = CmdUtils.valueOf(options, "je-log-size", 60);
        Integer nodeMax = CmdUtils.valueOf(options, "btree-nodemax", 512);

        AbstractBdbConversion conversion = null;
        try {
            if(fromFormat.equals("Base") && toFormat.equals("NewDup")) {
                conversion = new BdbConvertBaseToNewDup(storeName,
                                                        clusterXmlPath,
                                                        sourceEnvPath,
                                                        destEnvPath,
                                                        logFileSize,
                                                        nodeMax);
            } else if(fromFormat.equals("Base") && toFormat.equals("PidScan")) {
                conversion = new BdbConvertBaseToPidScan(storeName,
                                                         clusterXmlPath,
                                                         sourceEnvPath,
                                                         destEnvPath,
                                                         logFileSize,
                                                         nodeMax);

            } else if(fromFormat.equals("NewDup") && toFormat.equals("PidScan")) {
                conversion = new BdbConvertNewDupToPidScan(storeName,
                                                           clusterXmlPath,
                                                           sourceEnvPath,
                                                           destEnvPath,
                                                           logFileSize,
                                                           nodeMax);

            } else if(fromFormat.equals("PidScan") && toFormat.equals("NewDup")) {
                conversion = new BdbRevertPidScanToNewDup(storeName,
                                                          clusterXmlPath,
                                                          sourceEnvPath,
                                                          destEnvPath,
                                                          logFileSize,
                                                          nodeMax);

            } else if(fromFormat.equals("PidScan") && toFormat.equals("Base")) {
                conversion = new BdbRevertPidScanToBase(storeName,
                                                        clusterXmlPath,
                                                        sourceEnvPath,
                                                        destEnvPath,
                                                        logFileSize,
                                                        nodeMax);

            } else if(fromFormat.equals("NewDup") && toFormat.equals("Base")) {
                conversion = new BdbRevertNewDupToBase(storeName,
                                                       clusterXmlPath,
                                                       sourceEnvPath,
                                                       destEnvPath,
                                                       logFileSize,
                                                       nodeMax);
            } else {
                throw new Exception("Invalid conversion. Please check READMEFIRST file");
            }
            // start the actual data conversion
            conversion.transfer();
        } catch(Exception e) {
            logger.error("Error converting data", e);
        } finally {
            if(conversion != null)
                conversion.close();
        }
    }

    static boolean isValidFormat(String format) {
        if(format == null)
            return false;
        return format.equals("Base") || format.equals("NewDup") || format.equals("PidScan");
    }

    /**
     * Returns a Base64 encoded version of the byte array
     * 
     * @param key
     * @return
     */
    static String writeAsciiString(byte[] bytes) {
        return new String(Base64.encodeBase64(bytes));
    }

    static int abs(int a) {
        if(a >= 0)
            return a;
        else if(a != Integer.MIN_VALUE)
            return -a;
        return Integer.MAX_VALUE;
    }
}
