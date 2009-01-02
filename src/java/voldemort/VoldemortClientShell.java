package voldemort;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Node;
import voldemort.serialization.SerializationException;
import voldemort.serialization.json.JsonReader;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

/**
 * A toy shell to interact with the server via the command line
 * 
 * @author jay
 * 
 */
public class VoldemortClientShell {

    private static final String PROMPT = "> ";

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3)
            Utils.croak("USAGE: java VoldemortClientShell store_name bootstrap_url [command_file]");

        String storeName = args[0];
        String bootstrapUrl = args[1];
        BufferedReader reader = null;
        try {
            if (args.length == 3)
                reader = new BufferedReader(new FileReader(args[2]));
            else
                reader = new BufferedReader(new InputStreamReader(System.in));
        } catch (IOException e) {
            Utils.croak("Failure to open input stream: " + e.getMessage());
        }

        StoreClientFactory factory = new SocketStoreClientFactory(Executors.newFixedThreadPool(5),
                                                                  3,
                                                                  10,
                                                                  2000,
                                                                  2000,
                                                                  2000,
                                                                  bootstrapUrl);
        DefaultStoreClient<Object, Object> client = null;
        try {
            client = (DefaultStoreClient<Object, Object>) factory.getStoreClient(storeName);
        } catch (Exception e) {
            Utils.croak("Could not connect to server: " + e.getMessage());
        }

        System.out.println("Established connection to " + storeName + " via " + bootstrapUrl);
        System.out.print(PROMPT);
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            if (line.trim().equals(""))
                continue;
            try {
                if (line.toLowerCase().startsWith("put")) {
                    JsonReader jsonReader = new JsonReader(new StringReader(line.substring("put".length())));
                    client.put(tightenNumericTypes(jsonReader.read()),
                               tightenNumericTypes(jsonReader.read()));
                } else if (line.toLowerCase().startsWith("get")) {
                    JsonReader jsonReader = new JsonReader(new StringReader(line.substring("get".length())));
                    printVersioned(client.get(tightenNumericTypes(jsonReader.read())));
                } else if (line.toLowerCase().startsWith("delete")) {
                    JsonReader jsonReader = new JsonReader(new StringReader(line.substring("delete".length())));
                    client.delete(tightenNumericTypes(jsonReader.read()));
                } else if (line.startsWith("locate")) {
                    JsonReader jsonReader = new JsonReader(new StringReader(line.substring("locate".length())));
                    Object key = tightenNumericTypes(jsonReader.read());
                    printNodeList(client.getResponsibleNodes(key));
                } else if (line.startsWith("help")) {
                    System.out.println("Commands:");
                    System.out.println("put key value -- Associate the given value with the key.");
                    System.out.println("get key -- Retrieve the value associated with the key.");
                    System.out.println("delete key -- Remove all values associated with the key.");
                    System.out.println("locate key -- Determine which servers host the give key.");
                    System.out.println("help -- Print this message.");
                    System.out.println("exit -- Exit from this shell.");
                    System.out.println();
                } else if (line.startsWith("quit") || line.startsWith("exit")) {
                    System.out.println("k k thx bye.");
                    System.exit(0);
                } else {
                    System.err.println("Invalid command.");
                }
            } catch (SerializationException e) {
                System.err.print("Error serializing values: ");
                System.err.println(e.getMessage());
            } catch (VoldemortException e) {
                System.err.println("Exception thrown during operation.");
                e.printStackTrace(System.err);
            } catch (ArrayIndexOutOfBoundsException e) {
                System.err.println("Invalid command.");
            } catch (Exception e) {
                System.err.println("Unexpected error:");
                e.printStackTrace(System.err);
            }
            System.out.print(PROMPT);
        }
    }

    private static void printNodeList(List<Node> nodes) {
        if (nodes.size() > 0) {
            for (int i = 0; i < nodes.size(); i++) {
                Node node = nodes.get(i);
                System.out.println("Node " + node.getId());
                System.out.println("host:  " + node.getHost());
                System.out.println("port: " + node.getSocketPort());
                System.out.println("available: " + (node.getStatus().isAvailable() ? "yes" : "no"));
                System.out.println("last checked: " + node.getStatus().getMsSinceLastCheck()
                                   + " ms ago");
                System.out.println();
            }
        }
    }

    private static void printVersioned(Versioned<Object> v) {
        if (v == null) {
            System.out.println("null");
        } else {
            System.out.print(v.getVersion());
            System.out.print(": ");
            printObject(v.getValue());
            System.out.println();
        }
    }

    @SuppressWarnings("unchecked")
    private static void printObject(Object o) {
        if (o == null) {
            System.out.print("null");
        } else if (o instanceof String) {
            System.out.print('"');
            System.out.print(o);
            System.out.print('"');
        } else if (o instanceof Date) {
            DateFormat df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);
            System.out.print("'");
            System.out.print(df.format((Date) o));
            System.out.print("'");
        } else if (o instanceof List) {
            List<Object> l = (List<Object>) o;
            System.out.print("[");
            for (Object obj : l)
                printObject(obj);
            System.out.print("]");
        } else if (o instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) o;
            System.out.print('{');
            for (String s : m.keySet()) {
                printObject(s);
                System.out.print(':');
                printObject(m.get(s));
                System.out.print(", ");
            }
            System.out.print('}');
        } else {
            System.out.print(o);
        }
    }

    /*
     * We need to coerce numbers to the tightest possible type and let the
     * schema coerce them to the proper
     */
    @SuppressWarnings("unchecked")
    private static Object tightenNumericTypes(Object o) {
        if (o == null) {
            return null;
        } else if (o instanceof List) {
            List l = (List) o;
            for (int i = 0; i < l.size(); i++)
                l.set(i, tightenNumericTypes(l.get(i)));
            return l;
        } else if (o instanceof Map) {
            Map m = (Map) o;
            for (Map.Entry entry : (Set<Map.Entry>) m.entrySet())
                m.put(entry.getKey(), tightenNumericTypes(entry.getValue()));
            return m;
        } else if (o instanceof Number) {
            Number n = (Number) o;
            if (o instanceof Integer) {
                if (n.intValue() < Byte.MAX_VALUE)
                    return n.byteValue();
                else if (n.intValue() < Short.MAX_VALUE)
                    return n.shortValue();
                else
                    return n;
            } else if (o instanceof Double) {
                if (n.doubleValue() < Float.MAX_VALUE)
                    return n.floatValue();
                else
                    return n;
            } else {
                throw new RuntimeException("Unsupported numeric type: " + o.getClass());
            }
        } else {
            return o;
        }
    }

}
