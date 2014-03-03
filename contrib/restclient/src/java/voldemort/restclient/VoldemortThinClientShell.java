package voldemort.restclient;

/*
 * Copyright 2008-2013 LinkedIn, Inc
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.lang.mutable.MutableInt;

import voldemort.VoldemortClientShell;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.serialization.SerializationException;
import voldemort.serialization.json.EndOfFileException;
import voldemort.utils.Utils;

/**
 * Shell to interact with the voldemort coordinator from the command line...
 * 
 */
public class VoldemortThinClientShell extends VoldemortClientShell {

    private RESTClientFactory restClientFactory;
    String storeName;

    public VoldemortThinClientShell(String storeName,
                                    String bootstrapUrl,
                                    BufferedReader commandReader,
                                    PrintStream commandOutput,
                                    PrintStream errorStream) {
        super(commandReader, commandOutput, errorStream);
        this.storeName = storeName;
        Properties properties = new Properties();
        properties.setProperty(ClientConfig.BOOTSTRAP_URLS_PROPERTY, bootstrapUrl);
        properties.setProperty(ClientConfig.ROUTING_TIMEOUT_MS_PROPERTY, "1500");
        RESTClientFactory.Config mainConfig = new RESTClientFactory.Config(properties, null);
        restClientFactory = new RESTClientFactory(mainConfig);
        this.client = restClientFactory.getStoreClient(storeName);
    }

    @Override
    protected void safeClose() {
        if(restClientFactory != null) {
            restClientFactory.close();
        }
    }

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSet options = parser.parse(args);

        List<String> nonOptions = options.nonOptionArguments();
        if(nonOptions.size() < 2 || nonOptions.size() > 3) {
            System.err.println("Usage: java VoldemortThinClientShell store_name coordinator_url [command_file] [options]");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        String storeName = nonOptions.get(0);
        String bootstrapUrl = nonOptions.get(1);
        BufferedReader inputReader = null;
        boolean fileInput = false;

        try {
            if(nonOptions.size() == 3) {
                inputReader = new BufferedReader(new FileReader(nonOptions.get(2)));
                fileInput = true;
            } else {
                inputReader = new BufferedReader(new InputStreamReader(System.in));
            }
        } catch(IOException e) {
            Utils.croak("Failure to open input stream: " + e.getMessage());
        }
        VoldemortThinClientShell shell = new VoldemortThinClientShell(storeName,
                                                                      bootstrapUrl,
                                                                      inputReader,
                                                                      System.out,
                                                                      System.err);

        shell.process(fileInput);
    }

    @Override
    protected Object parseKey(String argStr, MutableInt parsePos) {
        return parseObject(restClientFactory.getKeySerializer(this.storeName), argStr, parsePos);
    }

    @Override
    protected Object parseValue(String argStr, MutableInt parsePos) {
        return parseObject(restClientFactory.getValueSerializer(this.storeName), argStr, parsePos);
    }

    @Override
    protected void processCommands(boolean printCommands) throws IOException {
        for(String line = commandReader.readLine(); line != null; line = commandReader.readLine()) {
            if(line.trim().equals(""))
                continue;
            if(printCommands)
                commandOutput.println(line);
            try {
                if(line.toLowerCase().startsWith("put")) {
                    processPut(line.substring("put".length()));
                } else if(line.toLowerCase().startsWith("getall")) {
                    processGetAll(line.substring("getall".length()));
                } else if(line.toLowerCase().startsWith("get")) {
                    processGet(line.substring("get".length()));
                } else if(line.toLowerCase().startsWith("delete")) {
                    processDelete(line.substring("delete".length()));
                } else if(line.startsWith("help")) {
                    commandOutput.println();
                    commandOutput.println("Commands:");
                    commandOutput.println(PROMPT
                                          + "put key value --- Associate the given value with the key.");
                    commandOutput.println(PROMPT
                                          + "get key --- Retrieve the value associated with the key.");
                    commandOutput.println(PROMPT
                                          + "getall key1 [key2...] --- Retrieve the value(s) associated with the key(s).");
                    commandOutput.println(PROMPT
                                          + "delete key --- Remove all values associated with the key.");
                    commandOutput.println(PROMPT + "help --- Print this message.");
                    commandOutput.println(PROMPT + "exit --- Exit from this shell.");
                    commandOutput.println();
                    commandOutput.println("Avro usage:");
                    commandOutput.println("For avro keys or values, ensure that the entire json string is enclosed within single quotes (').");
                    commandOutput.println("Also, the field names and strings should STRICTLY be enclosed by double quotes(\")");
                    commandOutput.println("eg: > put '{\"id\":1,\"name\":\"Vinoth Chandar\"}' '[{\"skill\":\"java\", \"score\":90.27, \"isendorsed\": true}]'");

                } else if(line.equals("quit") || line.equals("exit")) {
                    commandOutput.println("bye.");
                    System.exit(0);
                } else {
                    errorStream.println("Invalid command. (Try 'help' for usage.)");
                }
            } catch(EndOfFileException e) {
                errorStream.println("Expected additional token.");
            } catch(SerializationException e) {
                errorStream.print("Error serializing values: ");
                e.printStackTrace(errorStream);
            } catch(VoldemortException e) {
                errorStream.println("Exception thrown during operation.");
                e.printStackTrace(errorStream);
            } catch(ArrayIndexOutOfBoundsException e) {
                errorStream.println("Invalid command. (Try 'help' for usage.)");
            } catch(Exception e) {
                errorStream.println("Unexpected error:");
                e.printStackTrace(errorStream);
            }
            commandOutput.print(PROMPT);
        }
    }
}
