package voldemort.tools;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.StoreDefinition;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.xml.StoreDefinitionsMapper;

public class ValidateSchemaForAvroStores {

    /*-
     * 
     * Input: 
     * 1. Absolute path to a Directory containing multiple stores.xml with no other type of files (OR)
     * 2. Absolute path to a single stores.xml file
     * 
     * NOTE:
     *    1. Please name your files meaningfully so you know which stores.xml it is.
     *    2. This tool just checks for invalid, partial and non backwards compatible Avro schemas and
     *       that it DOESNOT fix invalid/partial/non backwards-compatible schemas
     *    3. It is users responsibility to give right stores.xml files. Exceptions will
     *       be thrown for other type of files since it is not possible to determine if the files are stores.xml or something else.
     *    4. Avro types validated: avro-generic and avro-generic-versioned
     *    5. Avro types for which backwards compatibility is checked: avro-generic-versioned
     *    6. Note that avro-specific and avro-reflective are not validated
     * 
     * How to check if a schema is valid?
     * Exceptions are thrown otherwise. Look for them and the corresponding "Store name: " 
     * 
     * How to know if a valid schema is fully backwards compatible?
     * Grep for "WARN" or "ERROR" in console output for partial/non-backwards compatible avro schemas. If no match for grep then things are good.
     * 
     * TODO: probably add ability to pick all stores.xml from a given directory's subdirectories and filter all other files
     */

    protected static final Logger logger = Logger.getLogger(ValidateSchemaForAvroStores.class);

    public static void validate(File storesXMLFile) {
        logger.info("Stores-xml file name: " + storesXMLFile.getName());
        StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();
        List<StoreDefinition> newStoreDefs = null;

        // parse the store definitions from the xml
        try {
            newStoreDefs = storeDefsMapper.readStoreList(storesXMLFile);
        } catch(IOException e) {
            logger.error("An IO Exception occured");
            e.printStackTrace();
        }

        // validate schema for each store as needed
        for(StoreDefinition storeDefinition: newStoreDefs) {
            try {
                StoreDefinitionUtils.validateSchemaAsNeeded(storeDefinition);
            } catch(VoldemortException ex) {
                logger.error("A VoldemortException occured");
                ex.printStackTrace();
            }
        }
    }

    public static void main(String args[]) {
        if(args.length < 1) {
            logger.error("Error in usage: \n 1. java ValidateSchemaForAvroStores <absolute path to your directory>     OR \n 2. java ValidateSchemaForAvroStores <absolute path to your stores.xml>");
            System.exit(-1);
        }
        logger.info("Validation starting...");
        File inputFile = new File(args[0]);
        if(inputFile.isDirectory()) {
            // if input is a directory
            try {
                File[] directoryListing = inputFile.listFiles();
                logger.info("Inside directory " + inputFile.getAbsolutePath());
                // validate for each stores.xml inside this directory
                for(File storesFile: directoryListing) {
                    if(storesFile.isFile()) {
                        validate(storesFile);
                    } else {
                        logger.error("Cannot check for files in sub directory - "
                                     + storesFile.getName()
                                     + "\n Please put all your stores.xml into one directory and retry");
                    }
                }
            } catch(Exception ex) {
                logger.error("Either no read permission for the directory or an I/O error occured");
                ex.printStackTrace();
            }
        } else {
            // if input is a file
            validate(inputFile);
        }
    }

}
