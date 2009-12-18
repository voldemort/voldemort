package voldemort.utils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import voldemort.VoldemortClientShell;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.serialization.json.EndOfFileException;
import voldemort.versioning.Versioned;
import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Toy shell for voldemort admin client
 *
 * @author afeinberg
 */
public class VoldemortAdminClientShell extends VoldemortClientShell {
    private static final String PROMPT = "> ";

    public static void main(String [] args) throws Exception {
        if (args.length < 1 || args.length > 2)
            Utils.croak("USAGE: java VoldemortAdminClientShell bootstrap_url [command_file]");

        String bootstrapUrl = args[0];
        String commandsFileName = "";
        BufferedReader fileReader = null;
        BufferedReader inputReader = null;
        try {
            if (args.length == 2) {
                commandsFileName = args[1];
                fileReader = new BufferedReader(new FileReader(commandsFileName));
            }
            inputReader = new BufferedReader(new InputStreamReader(System.in));
        } catch (IOException e) {
            Utils.croak("Failure to open input stream: " + e.getMessage());
        }

        AdminClient adminClient = null;
        try {
            adminClient = new AdminClient(bootstrapUrl, new AdminClientConfig());
        } catch (Exception e) {
            Utils.croak("Couldn't instantiate admin client: " + e.getMessage());
        }

        System.out.println("Created admin client to cluster at " + bootstrapUrl);
        System.out.print(PROMPT);
        if (fileReader != null) {
            processCommands(fileReader, adminClient, true);
            fileReader.close();
        }
        processCommands(inputReader, adminClient, false);


    }

    public static void processCommands(BufferedReader reader, AdminClient adminClient, boolean printCommands)
        throws IOException {
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            if (line.trim().equals(""))
                continue;
            if (printCommands)
                System.out.println(line);
            try {
                if (line.toLowerCase().startsWith("getmetadata")) {
                    String[] args = line.substring("getmetadata".length() + 1).split("\\s+");
                    int remoteNodeId = Integer.valueOf(args[0]);
                    String key = args[1];
                    Versioned<String> versioned = adminClient.getRemoteMetadata(remoteNodeId, key);
                    if (versioned == null) {
                        System.out.println("null");
                    } else {
                        System.out.println(versioned.getVersion());
                        System.out.print(": ");
                        System.out.println(versioned.getValue());
                        System.out.println();
                    }
                }
                else if (line.toLowerCase().startsWith("fetchkeys")) {
                    String[] args = line.substring("fetchkeys".length() + 1).split("\\s+");
                    int remoteNodeId = Integer.valueOf(args[0]);
                    String storeName = args[1];
                    List<Integer> partititionList = parseCsv(args[2]);
                    Iterator<ByteArray> partitionKeys =
                            adminClient.fetchKeys(remoteNodeId, storeName, partititionList, null);

                    BufferedWriter writer = null;
                    try {
                        if (args.length > 3) {
                            writer = new BufferedWriter(new FileWriter(new File(args[3])));
                        } else
                            writer = new BufferedWriter(new OutputStreamWriter(System.out));
                    } catch (IOException e) {
                        System.err.println("Failed to open the output stream");
                        e.printStackTrace();
                    }
                    if (writer != null) {
                        while (partitionKeys.hasNext()) {
                            ByteArray keyByteArray = partitionKeys.next();
                            StringBuilder lineBuilder = new StringBuilder();
                            lineBuilder.append(ByteUtils.getString(keyByteArray.get(), "UTF-8"));
                            lineBuilder.append("\n");
                            writer.write(lineBuilder.toString());
                        }
                        writer.flush();
                    }
                }
                else if (line.toLowerCase().startsWith("fetch")) {
                    String[] args = line.substring("fetch".length() + 1).split("\\s+");
                    int remoteNodeId = Integer.valueOf(args[0]);
                    String storeName = args[1];
                    List<Integer> partititionList = parseCsv(args[2]);
                    Iterator<Pair<ByteArray,Versioned<byte[]>>> partitionEntries =
                            adminClient.fetchEntries(remoteNodeId, storeName, partititionList, null);
                    BufferedWriter writer = null;
                    try {
                        if (args.length > 3) {
                            writer = new BufferedWriter(new FileWriter(new File(args[3])));
                        } else
                            writer = new BufferedWriter(new OutputStreamWriter(System.out));
                    } catch (IOException e) {
                        System.err.println("Failed to open the output stream");
                        e.printStackTrace();
                    }
                    if (writer != null) {
                        while (partitionEntries.hasNext()) {
                            Pair<ByteArray,Versioned<byte[]>> pair = partitionEntries.next();
                            ByteArray keyByteArray = pair.getFirst();
                            Versioned<byte[]> versioned = pair.getSecond();
                            StringBuilder lineBuilder = new StringBuilder();
                            lineBuilder.append(ByteUtils.getString(keyByteArray.get(), "UTF-8"));
                            lineBuilder.append("\t");
                            lineBuilder.append(versioned.getVersion());
                            lineBuilder.append("\t");
                            lineBuilder.append(ByteUtils.getString(versioned.getValue(), "UTF-8"));
                            lineBuilder.append("\n");
                            writer.write(lineBuilder.toString());
                        }
                        writer.flush();
                    }
                } else if (line.startsWith("help")) {
                    System.out.println("Commands:");
                    System.out.println("getmetadata node_id key -- Get metadata associated with key from node_id.");
                    System.out.println("fetchkeys node_id store_name partitions <file_name> -- Fetch all keys from given partitions" +
                            " (a comma separated list) of store_name on node_id. Optionally, write to file_name.");
                    System.out.println("fetch node_id store_name partitions <file_name> -- Fetch all entries from given partitions" +
                            " (a comma separated list) of store_name on node_id. Optionally, write to file_name.");
                    System.out.println("help -- Print this message.");
                    System.out.println("exit -- Exit from this shell.");
                    System.out.println();
                } else if (line.startsWith("quit") || line.startsWith("exit")) {
                    System.out.println("k k thx bye.");
                    System.exit(0);
                } else {
                    System.err.println("Invalid command.");
                }
            } catch (EndOfFileException e) {
                System.err.println("Expected additional token");
            } catch (VoldemortException e) {
                System.err.println("Exception thrown during operation.");
                e.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException e) {
                System.err.println("Invalid command.");
            } catch (Exception e) {
                System.err.println("Unexpected error:");
                e.printStackTrace(System.err);
            }
            System.out.print(PROMPT);
        }
    }

    private static List<Integer> parseCsv(String csv) {
        return Lists.transform(Arrays.asList(csv.split(",")),
                new Function<String, Integer> () {
                    public Integer apply(String input) {
                        return Integer.valueOf(input);
                    }
                });
    }
}
