package voldemort.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import voldemort.routing.RoutingStrategyType;
import voldemort.store.bdb.BdbStorageConfiguration;

/**
 * StoresGenerator generates the stores.xml file given either a store name along
 * with zone specific replication factors. Currently hard-coding the serializers
 * to string format
 * 
 */
public class StoresGenerator {

    public String createStoreDescriptor(String storeName,
                                        List<Integer> zoneRepFactor,
                                        int requiredReads,
                                        int requiredWrites,
                                        int zoneCountReads,
                                        int zoneCountWrites,
                                        String routingStrategy) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("<stores>");
        pw.println("\t<store>");
        pw.println("\t\t<name>" + storeName + "</name>");
        pw.println("\t\t<persistence>" + BdbStorageConfiguration.TYPE_NAME + "</persistence>");
        pw.println("\t\t<routing>client</routing>");
        pw.println("\t\t<routing-strategy>" + routingStrategy + "</routing-strategy>");

        pw.println("\t\t<key-serializer>");
        pw.println("\t\t\t<type>string</type>");
        pw.println("\t\t\t<schema-info>UTF-8</schema-info>");
        pw.println("\t\t</key-serializer>");
        pw.println("\t\t<value-serializer>");
        pw.println("\t\t\t<type>string</type>");
        pw.println("\t\t\t<schema-info>UTF-8</schema-info>");
        pw.println("\t\t</value-serializer>");

        pw.println("\t\t<required-reads>" + requiredReads + "</required-reads>");
        pw.println("\t\t<required-writes>" + requiredWrites + "</required-writes>");

        pw.println("\t\t<zone-replication-factor>");
        int zoneId = 0, totalReplicationFactor = 0;
        for(int repFactor: zoneRepFactor) {
            pw.println("\t\t\t<replication-factor zone-id=\"" + zoneId + "\">" + repFactor
                       + "</replication-factor>");
            zoneId++;
            totalReplicationFactor += repFactor;
        }
        pw.println("\t\t</zone-replication-factor>");
        pw.println("\t\t<replication-factor>" + totalReplicationFactor + "</replication-factor>");

        if(routingStrategy.compareTo(RoutingStrategyType.ZONE_STRATEGY) == 0) {
            pw.println("\t\t<zone-count-reads>" + zoneCountReads + "</zone-count-reads>");
            pw.println("\t\t<zone-count-writes>" + zoneCountWrites + "</zone-count-writes>");
        }
        pw.println("\t</store>");
        pw.println("</stores>");

        return sw.toString();
    }

}
