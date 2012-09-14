/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.xml;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.lang.StringUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.jdom.transform.JDOMSource;
import org.xml.sax.SAXException;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.utils.Utils;

/**
 * Parse a cluster xml file
 * 
 * 
 */
public class ClusterMapper {

    private static final String SERVER_ID_ELMT = "id";
    private static final String SERVER_PARTITIONS_ELMT = "partitions";
    private static final String ZONE_ELMT = "zone";
    private static final String ZONE_ID_ELMT = "zone-id";
    private static final String ZONE_PROXIMITY_LIST_ELMT = "proximity-list";
    private static final String SERVER_ELMT = "server";
    private static final String CLUSTER_NAME_ELMT = "name";
    private static final String CLUSTER_ELMT = "cluster";
    private static final String HOST_ELMT = "host";
    private static final String HTTP_PORT_ELMT = "http-port";
    private static final String SOCKET_PORT_ELMT = "socket-port";
    private static final String ADMIN_PORT_ELMT = "admin-port";

    public static final Integer MAX_PARTITIONID = 65535;

    private final Schema schema;

    public ClusterMapper() {
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Source source = new StreamSource(ClusterMapper.class.getResourceAsStream("cluster.xsd"));
            this.schema = factory.newSchema(source);
        } catch(SAXException e) {
            throw new MappingException(e);
        }
    }

    public Cluster readCluster(File f) throws IOException {
        FileReader reader = null;
        try {
            reader = new FileReader(f);
            return readCluster(reader);
        } finally {
            if(reader != null)
                reader.close();
        }
    }

    public Cluster readCluster(Reader input) {
        return readCluster(input, true);
    }

    @SuppressWarnings("unchecked")
    public Cluster readCluster(Reader input, boolean verifySchema) {
        try {
            SAXBuilder builder = new SAXBuilder(false);
            Document doc = builder.build(input);
            if(verifySchema) {
                Validator validator = this.schema.newValidator();
                validator.validate(new JDOMSource(doc));
            }
            Element root = doc.getRootElement();
            if(!root.getName().equals(CLUSTER_ELMT))
                throw new MappingException("Invalid root element: "
                                           + doc.getRootElement().getName());
            String name = root.getChildText(CLUSTER_NAME_ELMT);

            List<Zone> zones = new ArrayList<Zone>();
            for(Element node: (List<Element>) root.getChildren(ZONE_ELMT))
                zones.add(readZone(node));

            List<Node> servers = new ArrayList<Node>();
            for(Element node: (List<Element>) root.getChildren(SERVER_ELMT))
                servers.add(readServer(node));
            return new Cluster(name, servers, zones);
        } catch(JDOMException e) {
            throw new MappingException(e);
        } catch(SAXException e) {
            throw new MappingException(e);
        } catch(IOException e) {
            throw new MappingException(e);
        }
    }

    private Zone readZone(Element zone) {
        int zoneId = Integer.parseInt(zone.getChildText(ZONE_ID_ELMT));
        String proximityListTest = zone.getChildText(ZONE_PROXIMITY_LIST_ELMT).trim();
        LinkedList<Integer> proximityList = new LinkedList<Integer>();
        for(String node: Utils.COMMA_SEP.split(proximityListTest))
            if(node.trim().length() > 0)
                proximityList.add(Integer.parseInt(node.trim()));
        return new Zone(zoneId, proximityList);
    }

    public Node readServer(Element server) throws SAXException {
        int id = Integer.parseInt(server.getChildText(SERVER_ID_ELMT));
        String host = server.getChildText(HOST_ELMT);
        int httpPort = Integer.parseInt(server.getChildText(HTTP_PORT_ELMT));
        int socketPort = Integer.parseInt(server.getChildText(SOCKET_PORT_ELMT));

        // read admin-port default to -1 if not available
        int adminPort = (null != server.getChildText(ADMIN_PORT_ELMT)) ? Integer.parseInt(server.getChildText(ADMIN_PORT_ELMT))
                                                                      : -1;
        int zoneId = (null != server.getChildText(ZONE_ID_ELMT)) ? Integer.parseInt(server.getChildText(ZONE_ID_ELMT))
                                                                : Zone.DEFAULT_ZONE_ID;
        String partitionsText = server.getChildText(SERVER_PARTITIONS_ELMT).trim();
        List<Integer> partitions = new ArrayList<Integer>();
        for(String aPartition: Utils.COMMA_SEP.split(partitionsText))
            if(aPartition.trim().length() > 0) {
                Integer partition = Integer.parseInt(aPartition.trim());
                if(partition > MAX_PARTITIONID) {
                    throw new SAXException("Partition id cannot be greater than " + MAX_PARTITIONID);
                }
                partitions.add(partition);
            }

        return new Node(id, host, httpPort, socketPort, adminPort, zoneId, partitions);
    }

    public String writeCluster(Cluster cluster) {
        Document doc = new Document(new Element(CLUSTER_ELMT));
        doc.getRootElement().addContent(new Element(CLUSTER_NAME_ELMT).setText(cluster.getName()));
        boolean displayZones = cluster.getZones().size() > 1;
        if(displayZones) {
            for(Zone n: cluster.getZones())
                doc.getRootElement().addContent(mapZone(n));
        }
        for(Node n: cluster.getNodes())
            doc.getRootElement().addContent(mapServer(n, displayZones));
        XMLOutputter serializer = new XMLOutputter(Format.getPrettyFormat());
        return serializer.outputString(doc.getRootElement());
    }

    private Element mapZone(Zone zone) {
        Element zoneElement = new Element(ZONE_ELMT);
        zoneElement.addContent(new Element(ZONE_ID_ELMT).setText(Integer.toString(zone.getId())));
        String proximityListTest = StringUtils.join(zone.getProximityList().toArray(), ", ");
        zoneElement.addContent(new Element(ZONE_PROXIMITY_LIST_ELMT).setText(proximityListTest));
        return zoneElement;
    }

    private Element mapServer(Node node, boolean displayZones) {
        Element server = new Element(SERVER_ELMT);
        server.addContent(new Element(SERVER_ID_ELMT).setText(Integer.toString(node.getId())));
        server.addContent(new Element(HOST_ELMT).setText(node.getHost()));
        server.addContent(new Element(HTTP_PORT_ELMT).setText(Integer.toString(node.getHttpPort())));
        server.addContent(new Element(SOCKET_PORT_ELMT).setText(Integer.toString(node.getSocketPort())));
        server.addContent(new Element(ADMIN_PORT_ELMT).setText(Integer.toString(node.getAdminPort())));
        String serverPartitionsText = StringUtils.join(node.getPartitionIds().toArray(), ", ");
        server.addContent(new Element(SERVER_PARTITIONS_ELMT).setText(serverPartitionsText));
        if(displayZones)
            server.addContent(new Element(ZONE_ID_ELMT).setText(Integer.toString(node.getZoneId())));
        return server;
    }
}
