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

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import voldemort.client.RoutingTier;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StorageEngineType;
import voldemort.store.StoreDefinition;

/**
 * Parses a stores.xml file
 * 
 * @author jay
 * 
 */
public class StoreDefinitionsMapper {

    public final static String STORES_ELMT = "stores";
    public final static String STORE_ELMT = "store";
    public final static String STORE_NAME_ELMT = "name";
    public final static String STORE_PERSISTENCE_ELMT = "persistence";
    public final static String STORE_KEY_SERIALIZER_ELMT = "key-serializer";
    public final static String STORE_VALUE_SERIALIZER_ELMT = "value-serializer";
    public final static String STORE_SERIALIZATION_TYPE_ELMT = "type";
    public final static String STORE_SERIALIZATION_META_ELMT = "schema-info";
    public final static String STORE_ROUTING_TIER_ELMT = "routing";
    public final static String STORE_REPLICATION_FACTOR_ELMT = "replication-factor";
    public final static String STORE_REQUIRED_WRITES_ELMT = "required-writes";
    public final static String STORE_PREFERRED_WRITES_ELMT = "preferred-writes";
    public final static String STORE_REQUIRED_READS_ELMT = "required-reads";
    public final static String STORE_PREFERRED_READS_ELMT = "preferred-reads";
    public final static String STORE_RETENTION_POLICY_ELMT = "retention-days";
    private final static String STORE_VERSION_ATTR = "version";

    private final Validator validator;

    public StoreDefinitionsMapper() {
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Source source = new StreamSource(StoreDefinitionsMapper.class.getResourceAsStream("stores.xsd"));
            Schema schema = factory.newSchema(source);
            this.validator = schema.newValidator();
        } catch(SAXException e) {
            throw new MappingException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public List<StoreDefinition> readStoreList(Reader input) {
        try {

            SAXBuilder builder = new SAXBuilder();
            Document doc = builder.build(input);
            validator.validate(new JDOMSource(doc));
            Element root = doc.getRootElement();
            if(!root.getName().equals(STORES_ELMT))
                throw new MappingException("Invalid root element: "
                                           + doc.getRootElement().getName());
            List<StoreDefinition> stores = new ArrayList<StoreDefinition>();
            for(Element store: (List<Element>) root.getChildren(STORE_ELMT))
                stores.add(readStore(store));
            return stores;
        } catch(JDOMException e) {
            throw new MappingException(e);
        } catch(SAXException e) {
            throw new MappingException(e);
        } catch(IOException e) {
            throw new MappingException(e);
        }
    }

    public StoreDefinition readStore(Reader input) {
        SAXBuilder builder = new SAXBuilder();
        try {
            Document doc = builder.build(input);
            Element root = doc.getRootElement();
            return readStore(root);
        } catch(JDOMException e) {
            throw new MappingException(e);
        } catch(IOException e) {
            throw new MappingException(e);
        }
    }

    private StoreDefinition readStore(Element store) {
        String name = store.getChildText(STORE_NAME_ELMT);
        StorageEngineType storeType = StorageEngineType.fromDisplay(store.getChildText(STORE_PERSISTENCE_ELMT));
        int replicationFactor = Integer.parseInt(store.getChildText(STORE_REPLICATION_FACTOR_ELMT));
        int requiredReads = Integer.parseInt(store.getChildText(STORE_REQUIRED_READS_ELMT));
        int requiredWrites = Integer.parseInt(store.getChildText(STORE_REQUIRED_WRITES_ELMT));
        String preferredReadsStr = store.getChildText(STORE_PREFERRED_READS_ELMT);
        Integer preferredReads = null;
        if(preferredReadsStr != null)
            preferredReads = Integer.parseInt(preferredReadsStr);
        String preferredWritesStr = store.getChildText(STORE_PREFERRED_WRITES_ELMT);
        Integer preferredWrites = null;
        if(preferredWritesStr != null)
            preferredWrites = Integer.parseInt(preferredWritesStr);
        SerializerDefinition keySerializer = readSerializer(store.getChild(STORE_KEY_SERIALIZER_ELMT));
        if(keySerializer.getAllSchemaInfoVersions().size() > 1)
            throw new MappingException("Only a single schema is allowed for the store key.");
        SerializerDefinition valueSerializer = readSerializer(store.getChild(STORE_VALUE_SERIALIZER_ELMT));
        RoutingTier routingTier = RoutingTier.fromDisplay(store.getChildText(STORE_ROUTING_TIER_ELMT));
        Element retention = store.getChild(STORE_RETENTION_POLICY_ELMT);
        Integer retentionPolicyDays = null;
        if(retention != null)
            retentionPolicyDays = Integer.parseInt(retention.getText());

        return new StoreDefinition(name,
                                   storeType,
                                   keySerializer,
                                   valueSerializer,
                                   routingTier,
                                   replicationFactor,
                                   preferredReads,
                                   requiredReads,
                                   preferredWrites,
                                   requiredWrites,
                                   retentionPolicyDays);
    }

    private SerializerDefinition readSerializer(Element elmt) {
        String name = elmt.getChild(STORE_SERIALIZATION_TYPE_ELMT).getText();
        boolean hasVersion = true;
        Map<Integer, String> schemaInfosByVersion = new HashMap<Integer, String>();
        for(Object schemaInfo: elmt.getChildren(STORE_SERIALIZATION_META_ELMT)) {
            Element schemaInfoElmt = (Element) schemaInfo;
            String versionStr = schemaInfoElmt.getAttributeValue(STORE_VERSION_ATTR);
            int version;
            if(versionStr == null) {
                version = 0;
            } else if(versionStr.equals("none")) {
                version = 0;
                hasVersion = false;
            } else {
                version = Integer.parseInt(versionStr);
            }
            String info = schemaInfoElmt.getText();
            String previous = schemaInfosByVersion.put(version, info);
            if(previous != null)
                throw new MappingException("Duplicate version " + version
                                           + " found in schema info.");
        }
        if(!hasVersion && schemaInfosByVersion.size() > 1)
            throw new IllegalArgumentException("Specified multiple schemas AND version=none, which is not permitted.");

        return new SerializerDefinition(name, schemaInfosByVersion, hasVersion);
    }

    public String writeStoreList(List<StoreDefinition> stores) {
        Element root = new Element(STORES_ELMT);
        for(StoreDefinition def: stores)
            root.addContent(toElement(def));
        XMLOutputter serializer = new XMLOutputter(Format.getPrettyFormat());
        return serializer.outputString(root);
    }

    public String writeStore(StoreDefinition store) {
        XMLOutputter serializer = new XMLOutputter(Format.getPrettyFormat());
        return serializer.outputString(toElement(store));
    }

    private Element toElement(StoreDefinition storeDefinition) {
        Element store = new Element(STORE_ELMT);
        store.addContent(new Element(STORE_NAME_ELMT).setText(storeDefinition.getName()));
        store.addContent(new Element(STORE_PERSISTENCE_ELMT).setText(storeDefinition.getType()
                                                                                    .toDisplay()));
        store.addContent(new Element(STORE_ROUTING_TIER_ELMT).setText(storeDefinition.getRoutingPolicy()
                                                                                     .toDisplay()));
        store.addContent(new Element(STORE_REPLICATION_FACTOR_ELMT).setText(Integer.toString(storeDefinition.getReplicationFactor())));
        if(storeDefinition.getPreferredReads() != null)
            store.addContent(new Element(STORE_PREFERRED_READS_ELMT).setText(Integer.toString(storeDefinition.getPreferredReads())));
        store.addContent(new Element(STORE_REQUIRED_READS_ELMT).setText(Integer.toString(storeDefinition.getRequiredReads())));
        if(storeDefinition.getPreferredWrites() != null)
            store.addContent(new Element(STORE_PREFERRED_WRITES_ELMT).setText(Integer.toString(storeDefinition.getPreferredWrites())));
        store.addContent(new Element(STORE_REQUIRED_WRITES_ELMT).setText(Integer.toString(storeDefinition.getRequiredWrites())));

        Element keySerializer = new Element(STORE_KEY_SERIALIZER_ELMT);
        addSerializer(keySerializer, storeDefinition.getKeySerializer());
        store.addContent(keySerializer);

        Element valueSerializer = new Element(STORE_VALUE_SERIALIZER_ELMT);
        addSerializer(valueSerializer, storeDefinition.getValueSerializer());
        store.addContent(valueSerializer);

        if(storeDefinition.hasRetentionPeriod())
            store.addContent(new Element(STORE_RETENTION_POLICY_ELMT).setText(Integer.toString(storeDefinition.getRetentionDays())));

        return store;
    }

    private void addSerializer(Element parent, SerializerDefinition def) {
        parent.addContent(new Element(STORE_SERIALIZATION_TYPE_ELMT).setText(def.getName()));
        if(def.hasSchemaInfo()) {
            for(Map.Entry<Integer, String> entry: def.getAllSchemaInfoVersions().entrySet()) {
                Element schemaElmt = new Element(STORE_SERIALIZATION_META_ELMT);
                if(def.hasVersion())
                    schemaElmt.setAttribute(STORE_VERSION_ATTR, Integer.toString(entry.getKey()));
                else
                    schemaElmt.setAttribute(STORE_VERSION_ATTR, "none");
                schemaElmt.setText(entry.getValue());
                parent.addContent(schemaElmt);
            }
        }
    }

}
