package voldemort.store.readonly.mr.utils;

import java.io.StringReader;
import java.util.List;

import com.google.common.collect.Lists;

import voldemort.store.StoreDefinition;
import voldemort.utils.Utils;
import voldemort.xml.StoreDefinitionsMapper;

public class VoldemortUtils
{

  /**
   * Given the comma separated list of properties as a string, splits it multiple strings
   * 
   * @param paramValue
   *          Concatenated string
   * @param type
   *          Type of parameter ( to throw exception )
   * @return List of string properties
   */
  public static List<String> getCommaSeparatedStringValues(String paramValue, String type)
  {
    List<String> commaSeparatedProps = Lists.newArrayList();
    for (String url : Utils.COMMA_SEP.split(paramValue.trim()))
      if (url.trim().length() > 0)
        commaSeparatedProps.add(url);

    if (commaSeparatedProps.size() == 0)
    {
      throw new RuntimeException("Number of " + type + " should be greater than zero");
    }
    return commaSeparatedProps;
  }

  public static String getStoreDefXml(String storeName,
                                      int replicationFactor,
                                      int requiredReads,
                                      int requiredWrites,
                                      Integer preferredReads,
                                      Integer preferredWrites,
                                      String keySerializer,
                                      String valSerializer,
                                      String description,
                                      String owners)
  {
    StringBuffer storeXml = new StringBuffer();

    storeXml.append("<store>\n\t<name>");
    storeXml.append(storeName);
    storeXml.append("</name>\n\t<persistence>read-only</persistence>\n\t");
    if (description.length() != 0)
    {
      storeXml.append("<description>");
      storeXml.append(description);
      storeXml.append("</description>\n\t");
    }
    if (owners.length() != 0)
    {
      storeXml.append("<owners>");
      storeXml.append(owners);
      storeXml.append("</owners>\n\t");
    }
    storeXml.append("<routing>client</routing>\n\t<replication-factor>");
    storeXml.append(replicationFactor);
    storeXml.append("</replication-factor>\n\t<required-reads>");
    storeXml.append(requiredReads);
    storeXml.append("</required-reads>\n\t<required-writes>");
    storeXml.append(requiredWrites);
    storeXml.append("</required-writes>\n\t");
    if (preferredReads != null)
      storeXml.append("<preferred-reads>")
              .append(preferredReads)
              .append("</preferred-reads>\n\t");
    if (preferredWrites != null)
      storeXml.append("<preferred-writes>")
              .append(preferredWrites)
              .append("</preferred-writes>\n\t");
    storeXml.append("<key-serializer>");
    storeXml.append(keySerializer);
    storeXml.append("</key-serializer>\n\t<value-serializer>");
    storeXml.append(valSerializer);
    storeXml.append("</value-serializer>\n</store>");

    return storeXml.toString();
  }

  public static String getStoreDefXml(String storeName,
                                      int replicationFactor,
                                      int requiredReads,
                                      int requiredWrites,
                                      Integer preferredReads,
                                      Integer preferredWrites,
                                      String keySerializer,
                                      String valSerializer)
  {
    StringBuffer storeXml = new StringBuffer();

    storeXml.append("<store>\n\t<name>");
    storeXml.append(storeName);
    storeXml.append("</name>\n\t<persistence>read-only</persistence>\n\t<routing>client</routing>\n\t<replication-factor>");
    storeXml.append(replicationFactor);
    storeXml.append("</replication-factor>\n\t<required-reads>");
    storeXml.append(requiredReads);
    storeXml.append("</required-reads>\n\t<required-writes>");
    storeXml.append(requiredWrites);
    storeXml.append("</required-writes>\n\t");
    if (preferredReads != null)
      storeXml.append("<preferred-reads>")
              .append(preferredReads)
              .append("</preferred-reads>\n\t");
    if (preferredWrites != null)
      storeXml.append("<preferred-writes>")
              .append(preferredWrites)
              .append("</preferred-writes>\n\t");
    storeXml.append("<key-serializer>");
    storeXml.append(keySerializer);
    storeXml.append("</key-serializer>\n\t<value-serializer>");
    storeXml.append(valSerializer);
    storeXml.append("</value-serializer>\n</store>");

    return storeXml.toString();
  }

  public static StoreDefinition getStoreDef(String xml)
  {
    return new StoreDefinitionsMapper().readStore(new StringReader(xml));
  }
}
