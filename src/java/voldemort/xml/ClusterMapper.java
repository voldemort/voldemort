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
import java.util.List;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

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

import com.google.common.base.Join;

/**
 * Parse a cluster xml file
 * 
 * @author jay
 * 
 */
public class ClusterMapper {

    private static final Pattern COMMA_SEP = Pattern.compile("\\s*,\\s*");

    private static final String SERVER_ID_ELMT = "id";
    private static final String SERVER_PARTITIONS_ELMT = "partitions";
    private static final String SERVER_ELMT = "server";
    private static final String CLUSTER_NAME_ELMT = "name";
    private static final String CLUSTER_ELMT = "cluster";
    private static final String HOST_ELMT = "host";
    private static final String HTTP_PORT_ELMT = "http-port";
    private static final String SOCKET_PORT_ELMT = "socket-port";

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

    @SuppressWarnings("unchecked")
    public Cluster readCluster(Reader input) {
        try {
            SAXBuilder builder = new SAXBuilder(false);
            Document doc = builder.build(input);
            Validator validator = this.schema.newValidator();
            validator.validate(new JDOMSource(doc));
            Element root = doc.getRootElement();
            if(!root.getName().equals(CLUSTER_ELMT))
                throw new MappingException("Invalid root element: "
                                           + doc.getRootElement().getName());
            String name = root.getChildText(CLUSTER_NAME_ELMT);
            List<Node> nodes = new ArrayList<Node>();
            for(Element node: (List<Element>) root.getChildren(SERVER_ELMT))
                nodes.add(readServer(node));
            return new Cluster(name, nodes);
        } catch(JDOMException e) {
            throw new MappingException(e);
        } catch(SAXException e) {
            throw new MappingException(e);
        } catch(IOException e) {
            throw new MappingException(e);
        }
    }

    public Node readServer(Element server) {
        int id = Integer.parseInt(server.getChildText(SERVER_ID_ELMT));
        String host = server.getChildText(HOST_ELMT);
        int httpPort = Integer.parseInt(server.getChildText(HTTP_PORT_ELMT));
        int socketPort = Integer.parseInt(server.getChildText(SOCKET_PORT_ELMT));
        String partitionsText = server.getChildText(SERVER_PARTITIONS_ELMT).trim();
        List<Integer> partitions = new ArrayList<Integer>();
        for(String aPartition: COMMA_SEP.split(partitionsText))
            partitions.add(Integer.parseInt(aPartition.trim()));
        return new Node(id, host, httpPort, socketPort, partitions);
    }

    public String writeCluster(Cluster cluster) {
        Document doc = new Document(new Element(CLUSTER_ELMT));
        doc.getRootElement().addContent(new Element(CLUSTER_NAME_ELMT).setText(cluster.getName()));
        for(Node n: cluster.getNodes())
            doc.getRootElement().addContent(mapServer(n));
        XMLOutputter serializer = new XMLOutputter(Format.getPrettyFormat());
        return serializer.outputString(doc.getRootElement());
    }

    private Element mapServer(Node node) {
        Element server = new Element(SERVER_ELMT);
        server.addContent(new Element(SERVER_ID_ELMT).setText(Integer.toString(node.getId())));
        server.addContent(new Element(HOST_ELMT).setText(node.getHost()));
        server.addContent(new Element(HTTP_PORT_ELMT).setText(Integer.toString(node.getHttpPort())));
        server.addContent(new Element(SOCKET_PORT_ELMT).setText(Integer.toString(node.getSocketPort())));
        server.addContent(new Element(SERVER_PARTITIONS_ELMT).setText(Join.join(", ",
                                                                                node.getPartitionIds())));
        return server;
    }

}
