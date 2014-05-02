/*
 * Copyright 2014 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.tools;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * 
 * Input: 1. Absolute path to a Directory which may contain stores.xml in its
 * sub directories (OR) 2. Absolute path to a single stores.xml file
 * 
 * NOTE: 1. This tool just checks for invalid, partial and non backwards
 * compatible Avro schemas and that it DOESNOT fix invalid/partial/non
 * backwards-compatible schemas 2. It is users responsibility to give right
 * path. 3. Avro types validated: avro-generic and avro-generic-versioned 4.
 * Avro types for which backwards compatibility is checked:
 * avro-generic-versioned 6. Note that avro-specific and avro-reflective are not
 * validated
 * 
 * How to check if a schema is valid? Exceptions are thrown otherwise. Look for
 * them and the corresponding store in the log
 * 
 * How to know if a valid schema is fully backwards compatible? Grep for "WARN"
 * or "ERROR" in console output for partial/non-backwards compatible avro
 * schemas. If no match for grep then things are good.
 * 
 */
public class ValidateSchemaForAvroStores {

    protected static final Logger logger = Logger.getLogger(ValidateSchemaForAvroStores.class);

    public static void validate(File storesXMLFile) {
        StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();
        List<StoreDefinition> newStoreDefs = null;

        // parse the store definitions from the xml
        try {
            newStoreDefs = storeDefsMapper.readStoreList(storesXMLFile);
        } catch(Exception e) {
            e.printStackTrace();
            return;
        }

        // validate schema for each store as needed
        for(StoreDefinition storeDefinition: newStoreDefs) {
            try {
                StoreDefinitionUtils.validateSchemaAsNeeded(storeDefinition);
            } catch(VoldemortException ex) {
                logger.error("A VoldemortException occured");
                ex.printStackTrace();
                // Continue with the loop so we catch all invalid schemas at
                // once.
            }
        }
    }

    public static void recurseDirectory(File inputFile) {
        if(inputFile.isDirectory()) {
            // if input is a directory
            try {
                File[] directoryListing = inputFile.listFiles();
                logger.info("\n\nInside directory " + inputFile.getAbsolutePath());
                // validate for each stores.xml inside this directory
                for(File storesFile: directoryListing) {
                    if(storesFile.isFile() && storesFile.getName().equals(MetadataStore.STORES_KEY)) {
                        validate(storesFile);
                    } else {
                        recurseDirectory(storesFile);
                    }
                }

            } catch(Exception ex) {
                logger.error("Either no read permission for the directory or an I/O error occured");
                ex.printStackTrace();
            }
        } else {
            // if input is a file and is stores.xml else ignore
            if(inputFile.getName().equals(MetadataStore.STORES_KEY)) {
                validate(inputFile);
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
        recurseDirectory(inputFile);
    }

}
