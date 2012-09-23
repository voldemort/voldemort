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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import voldemort.client.RoutingTier;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.Compression;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.StoreUtils;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.system.SystemStoreConstants;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;

/**
 * Parses a stores.xml file
 * 
 * 
 */
public class StoreDefinitionsMapper {

    public final static String STORES_ELMT = "stores";
    public final static String STORE_ELMT = "store";
    public final static String STORE_DESCRIPTION_ELMT = "description";
    public final static String STORE_OWNERS_ELMT = "owners";
    public final static String STORE_NAME_ELMT = "name";
    public final static String STORE_PERSISTENCE_ELMT = "persistence";
    public final static String STORE_KEY_SERIALIZER_ELMT = "key-serializer";
    public final static String STORE_VALUE_SERIALIZER_ELMT = "value-serializer";
    public final static String STORE_TRANSFORM_SERIALIZER_ELMT = "transforms-serializer";
    public final static String STORE_SERIALIZATION_TYPE_ELMT = "type";
    public final static String STORE_SERIALIZATION_META_ELMT = "schema-info";
    public final static String STORE_COMPRESSION_ELMT = "compression";
    public final static String STORE_COMPRESSION_TYPE_ELMT = "type";
    public final static String STORE_COMPRESSION_OPTIONS_ELMT = "options";
    public final static String STORE_ROUTING_TIER_ELMT = "routing";
    public final static String STORE_REPLICATION_FACTOR_ELMT = "replication-factor";
    public final static String STORE_REQUIRED_WRITES_ELMT = "required-writes";
    public final static String STORE_PREFERRED_WRITES_ELMT = "preferred-writes";
    public final static String STORE_REQUIRED_READS_ELMT = "required-reads";
    public final static String STORE_PREFERRED_READS_ELMT = "preferred-reads";
    public final static String STORE_RETENTION_POLICY_ELMT = "retention-days";
    public final static String STORE_RETENTION_FREQ_ELMT = "retention-frequency";
    public final static String STORE_RETENTION_SCAN_THROTTLE_RATE_ELMT = "retention-scan-throttle-rate";
    public final static String STORE_ROUTING_STRATEGY = "routing-strategy";
    public final static String STORE_ZONE_ID_ELMT = "zone-id";
    public final static String STORE_ZONE_REPLICATION_FACTOR_ELMT = "zone-replication-factor";
    public final static String STORE_ZONE_COUNT_READS = "zone-count-reads";
    public final static String STORE_ZONE_COUNT_WRITES = "zone-count-writes";
    public final static String HINTED_HANDOFF_STRATEGY = "hinted-handoff-strategy";
    public final static String HINT_PREFLIST_SIZE = "hint-preflist-size";
    public final static String VIEW_ELMT = "view";
    public final static String VIEW_TARGET_ELMT = "view-of";
    public final static String VIEW_TRANS_ELMT = "view-class";
    public final static String VIEW_SERIALIZER_FACTORY_ELMT = "view-serializer-factory";
    private final static String STORE_VERSION_ATTR = "version";
    private final static String STORE_MEMORY_FOOTPRINT = "memory-footprint";

    private final Schema schema;

    public StoreDefinitionsMapper() {
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Source source = new StreamSource(StoreDefinitionsMapper.class.getResourceAsStream("stores.xsd"));
            this.schema = factory.newSchema(source);
        } catch(SAXException e) {
            throw new MappingException(e);
        }
    }

    public List<StoreDefinition> readStoreList(File f) throws IOException {
        FileReader reader = null;
        try {
            reader = new FileReader(f);
            return readStoreList(reader);
        } finally {
            if(reader != null)
                reader.close();
        }
    }

    public List<StoreDefinition> readStoreList(Reader input) {
        return readStoreList(input, true);
    }

    public List<StoreDefinition> readStoreList(Reader input, boolean verifySchema) {
        try {

            SAXBuilder builder = new SAXBuilder();
            Document doc = builder.build(input);
            if(verifySchema) {
                Validator validator = schema.newValidator();
                validator.validate(new JDOMSource(doc));
            }
            Element root = doc.getRootElement();
            if(!root.getName().equals(STORES_ELMT))
                throw new MappingException("Invalid root element: "
                                           + doc.getRootElement().getName());
            List<StoreDefinition> stores = new ArrayList<StoreDefinition>();
            for(Object store: root.getChildren(STORE_ELMT))
                stores.add(readStore((Element) store));
            for(Object view: root.getChildren(VIEW_ELMT))
                stores.add(readView((Element) view, stores));
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

    @SuppressWarnings("unchecked")
    private StoreDefinition readStore(Element store) {
        String name = store.getChildText(STORE_NAME_ELMT);
        String storeType = store.getChildText(STORE_PERSISTENCE_ELMT);
        String description = store.getChildText(STORE_DESCRIPTION_ELMT);
        String ownerText = store.getChildText(STORE_OWNERS_ELMT);
        List<String> owners = Lists.newArrayList();
        if(ownerText != null) {
            for(String owner: Utils.COMMA_SEP.split(ownerText.trim()))
                if(owner.trim().length() > 0)
                    owners.add(owner);
        }
        int replicationFactor = Integer.parseInt(store.getChildText(STORE_REPLICATION_FACTOR_ELMT));
        HashMap<Integer, Integer> zoneReplicationFactor = null;
        Element zoneReplicationFactorNode = store.getChild(STORE_ZONE_REPLICATION_FACTOR_ELMT);
        if(zoneReplicationFactorNode != null) {
            zoneReplicationFactor = new HashMap<Integer, Integer>();
            for(Element node: (List<Element>) zoneReplicationFactorNode.getChildren(STORE_REPLICATION_FACTOR_ELMT)) {
                int zone = Integer.parseInt(node.getAttribute(STORE_ZONE_ID_ELMT).getValue());
                int repFactor = Integer.parseInt(node.getText());
                zoneReplicationFactor.put(zone, repFactor);
            }
        }
        String zoneCountReadsStr = store.getChildText(STORE_ZONE_COUNT_READS);
        Integer zoneCountReads = null;
        if(zoneCountReadsStr != null)
            zoneCountReads = Integer.parseInt(zoneCountReadsStr);

        String zoneCountWritesStr = store.getChildText(STORE_ZONE_COUNT_WRITES);
        Integer zoneCountWrites = null;
        if(zoneCountWritesStr != null)
            zoneCountWrites = Integer.parseInt(zoneCountWritesStr);

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

        String routingStrategyType = (null != store.getChildText(STORE_ROUTING_STRATEGY)) ? store.getChildText(STORE_ROUTING_STRATEGY)
                                                                                         : RoutingStrategyType.CONSISTENT_STRATEGY;

        Element retention = store.getChild(STORE_RETENTION_POLICY_ELMT);
        Integer retentionPolicyDays = null;
        Integer retentionThrottleRate = null;
        Integer retentionFreqDays = null;
        if(retention != null) {
            retentionPolicyDays = Integer.parseInt(retention.getText());
            Element throttleRate = store.getChild(STORE_RETENTION_SCAN_THROTTLE_RATE_ELMT);
            if(throttleRate != null)
                retentionThrottleRate = Integer.parseInt(throttleRate.getText());
            Element retentionFreqDaysElement = store.getChild(STORE_RETENTION_FREQ_ELMT);
            if(retentionFreqDaysElement != null)
                retentionFreqDays = Integer.parseInt(retentionFreqDaysElement.getText());
        }

        if(routingStrategyType.compareTo(RoutingStrategyType.ZONE_STRATEGY) == 0
           && !SystemStoreConstants.isSystemStore(name)) {
            if(zoneCountReads == null || zoneCountWrites == null || zoneReplicationFactor == null) {
                throw new MappingException("Have not set one of the following correctly for store '"
                                           + name
                                           + "' - "
                                           + STORE_ZONE_COUNT_READS
                                           + ", "
                                           + STORE_ZONE_COUNT_WRITES
                                           + ", "
                                           + STORE_ZONE_REPLICATION_FACTOR_ELMT);
            }
        }

        HintedHandoffStrategyType hintedHandoffStrategy = null;
        if(store.getChildText(HINTED_HANDOFF_STRATEGY) != null)
            hintedHandoffStrategy = HintedHandoffStrategyType.fromDisplay(store.getChildText(HINTED_HANDOFF_STRATEGY));

        String hintPrefListSizeStr = store.getChildText(HINT_PREFLIST_SIZE);
        Integer hintPrefListSize = (null != hintPrefListSizeStr) ? Integer.parseInt(hintPrefListSizeStr)
                                                                : null;

        String memoryFootprintStr = store.getChildText(STORE_MEMORY_FOOTPRINT);
        long memoryFootprintMB = 0;
        if(memoryFootprintStr != null)
            memoryFootprintMB = Long.parseLong(memoryFootprintStr);

        return new StoreDefinitionBuilder().setName(name)
                                           .setType(storeType)
                                           .setDescription(description)
                                           .setOwners(owners)
                                           .setKeySerializer(keySerializer)
                                           .setValueSerializer(valueSerializer)
                                           .setRoutingPolicy(routingTier)
                                           .setRoutingStrategyType(routingStrategyType)
                                           .setReplicationFactor(replicationFactor)
                                           .setPreferredReads(preferredReads)
                                           .setRequiredReads(requiredReads)
                                           .setPreferredWrites(preferredWrites)
                                           .setRequiredWrites(requiredWrites)
                                           .setRetentionPeriodDays(retentionPolicyDays)
                                           .setRetentionScanThrottleRate(retentionThrottleRate)
                                           .setRetentionFrequencyDays(retentionFreqDays)
                                           .setZoneReplicationFactor(zoneReplicationFactor)
                                           .setZoneCountReads(zoneCountReads)
                                           .setZoneCountWrites(zoneCountWrites)
                                           .setHintedHandoffStrategy(hintedHandoffStrategy)
                                           .setHintPrefListSize(hintPrefListSize)
                                           .setMemoryFootprintMB(memoryFootprintMB)
                                           .build();
    }

    private StoreDefinition readView(Element store, List<StoreDefinition> stores) {
        String name = store.getChildText(STORE_NAME_ELMT);
        String targetName = store.getChildText(VIEW_TARGET_ELMT);
        String description = store.getChildText(STORE_DESCRIPTION_ELMT);
        String ownerText = store.getChildText(STORE_OWNERS_ELMT);
        List<String> owners = Lists.newArrayList();
        if(ownerText != null) {
            for(String owner: Utils.COMMA_SEP.split(ownerText.trim()))
                if(owner.trim().length() > 0)
                    owners.add(owner);
        }
        StoreDefinition target = StoreUtils.getStoreDef(stores, targetName);
        if(target == null)
            throw new MappingException("View \"" + name + "\" has target store \"" + targetName
                                       + "\" but no such store exists");

        int requiredReads = getChildWithDefault(store,
                                                STORE_REQUIRED_READS_ELMT,
                                                target.getRequiredReads());
        int preferredReads = getChildWithDefault(store,
                                                 STORE_PREFERRED_READS_ELMT,
                                                 target.getRequiredReads());
        int requiredWrites = getChildWithDefault(store,
                                                 STORE_REQUIRED_WRITES_ELMT,
                                                 target.getRequiredReads());
        int preferredWrites = getChildWithDefault(store,
                                                  STORE_PREFERRED_WRITES_ELMT,
                                                  target.getRequiredReads());
        Integer zoneCountReads = getChildWithDefault(store,
                                                     STORE_ZONE_COUNT_READS,
                                                     target.getZoneCountReads());
        Integer zoneCountWrites = getChildWithDefault(store,
                                                      STORE_ZONE_COUNT_WRITES,
                                                      target.getZoneCountWrites());

        String viewSerializerFactoryName = null;
        if(store.getChildText(VIEW_SERIALIZER_FACTORY_ELMT) != null) {
            viewSerializerFactoryName = store.getChild(VIEW_SERIALIZER_FACTORY_ELMT).getText();
        }

        SerializerDefinition keySerializer = target.getKeySerializer();
        SerializerDefinition valueSerializer = target.getValueSerializer();
        if(store.getChild(STORE_VALUE_SERIALIZER_ELMT) != null)
            valueSerializer = readSerializer(store.getChild(STORE_VALUE_SERIALIZER_ELMT));

        SerializerDefinition transformSerializer = target.getTransformsSerializer();
        if(store.getChild(STORE_TRANSFORM_SERIALIZER_ELMT) != null)
            transformSerializer = readSerializer(store.getChild(STORE_TRANSFORM_SERIALIZER_ELMT));

        RoutingTier routingTier = null;
        if(store.getChildText(STORE_ROUTING_TIER_ELMT) != null) {
            routingTier = RoutingTier.fromDisplay(store.getChildText(STORE_ROUTING_TIER_ELMT));
        } else {
            routingTier = target.getRoutingPolicy();
        }

        String viewClass = store.getChildText(VIEW_TRANS_ELMT);

        return new StoreDefinitionBuilder().setName(name)
                                           .setViewOf(targetName)
                                           .setType(ViewStorageConfiguration.TYPE_NAME)
                                           .setDescription(description)
                                           .setOwners(owners)
                                           .setRoutingPolicy(routingTier)
                                           .setRoutingStrategyType(target.getRoutingStrategyType())
                                           .setKeySerializer(keySerializer)
                                           .setValueSerializer(valueSerializer)
                                           .setTransformsSerializer(transformSerializer)
                                           .setReplicationFactor(target.getReplicationFactor())
                                           .setZoneReplicationFactor(target.getZoneReplicationFactor())
                                           .setPreferredReads(preferredReads)
                                           .setRequiredReads(requiredReads)
                                           .setPreferredWrites(preferredWrites)
                                           .setRequiredWrites(requiredWrites)
                                           .setZoneCountReads(zoneCountReads)
                                           .setZoneCountWrites(zoneCountWrites)
                                           .setView(viewClass)
                                           .setSerializerFactory(viewSerializerFactoryName)
                                           .build();
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

        Element compressionElmt = elmt.getChild(STORE_COMPRESSION_ELMT);
        Compression compression = null;
        if(compressionElmt != null)
            compression = new Compression(compressionElmt.getChildText("type"),
                                          compressionElmt.getChildText("options"));
        return new SerializerDefinition(name, schemaInfosByVersion, hasVersion, compression);
    }

    public String writeStoreList(List<StoreDefinition> stores) {
        Element root = new Element(STORES_ELMT);
        for(StoreDefinition def: stores) {
            if(def.isView())
                root.addContent(viewToElement(def));
            else
                root.addContent(storeToElement(def));
        }
        XMLOutputter serializer = new XMLOutputter(Format.getPrettyFormat());
        return serializer.outputString(root);
    }

    public String writeStore(StoreDefinition store) {
        XMLOutputter serializer = new XMLOutputter(Format.getPrettyFormat());
        if(store.isView())
            return serializer.outputString(viewToElement(store));
        else
            return serializer.outputString(storeToElement(store));
    }

    private Element storeToElement(StoreDefinition storeDefinition) {
        Element store = new Element(STORE_ELMT);
        store.addContent(new Element(STORE_NAME_ELMT).setText(storeDefinition.getName()));
        store.addContent(new Element(STORE_PERSISTENCE_ELMT).setText(storeDefinition.getType()));
        if(storeDefinition.getDescription() != null)
            store.addContent(new Element(STORE_DESCRIPTION_ELMT).setText(storeDefinition.getDescription()));
        if(storeDefinition.getOwners() != null && storeDefinition.getOwners().size() > 0) {
            String ownersText = StringUtils.join(storeDefinition.getOwners().toArray(), ", ");
            store.addContent(new Element(STORE_OWNERS_ELMT).setText(ownersText));
        }
        store.addContent(new Element(STORE_ROUTING_STRATEGY).setText(storeDefinition.getRoutingStrategyType()));
        store.addContent(new Element(STORE_ROUTING_TIER_ELMT).setText(storeDefinition.getRoutingPolicy()
                                                                                     .toDisplay()));
        store.addContent(new Element(STORE_REPLICATION_FACTOR_ELMT).setText(Integer.toString(storeDefinition.getReplicationFactor())));
        HashMap<Integer, Integer> zoneReplicationFactor = storeDefinition.getZoneReplicationFactor();
        if(zoneReplicationFactor != null) {
            Element zoneReplicationFactorNode = new Element(STORE_ZONE_REPLICATION_FACTOR_ELMT);
            for(Integer zone: zoneReplicationFactor.keySet()) {
                zoneReplicationFactorNode.addContent(new Element(STORE_REPLICATION_FACTOR_ELMT).setText(Integer.toString(zoneReplicationFactor.get(zone)))
                                                                                               .setAttribute(STORE_ZONE_ID_ELMT,
                                                                                                             Integer.toString(zone)));
            }
            store.addContent(zoneReplicationFactorNode);
        }
        if(storeDefinition.hasPreferredReads())
            store.addContent(new Element(STORE_PREFERRED_READS_ELMT).setText(Integer.toString(storeDefinition.getPreferredReads())));
        store.addContent(new Element(STORE_REQUIRED_READS_ELMT).setText(Integer.toString(storeDefinition.getRequiredReads())));
        if(storeDefinition.hasPreferredWrites())
            store.addContent(new Element(STORE_PREFERRED_WRITES_ELMT).setText(Integer.toString(storeDefinition.getPreferredWrites())));
        store.addContent(new Element(STORE_REQUIRED_WRITES_ELMT).setText(Integer.toString(storeDefinition.getRequiredWrites())));
        if(storeDefinition.hasZoneCountReads())
            store.addContent(new Element(STORE_ZONE_COUNT_READS).setText(Integer.toString(storeDefinition.getZoneCountReads())));
        if(storeDefinition.hasZoneCountWrites())
            store.addContent(new Element(STORE_ZONE_COUNT_WRITES).setText(Integer.toString(storeDefinition.getZoneCountWrites())));

        if(storeDefinition.hasHintedHandoffStrategyType())
            store.addContent(new Element(HINTED_HANDOFF_STRATEGY).setText(storeDefinition.getHintedHandoffStrategyType()
                                                                                         .toDisplay()));
        if(storeDefinition.hasHintPreflistSize())
            store.addContent(new Element(HINT_PREFLIST_SIZE).setText(Integer.toString(storeDefinition.getHintPrefListSize())));

        Element keySerializer = new Element(STORE_KEY_SERIALIZER_ELMT);
        addSerializer(keySerializer, storeDefinition.getKeySerializer());
        store.addContent(keySerializer);

        Element valueSerializer = new Element(STORE_VALUE_SERIALIZER_ELMT);
        addSerializer(valueSerializer, storeDefinition.getValueSerializer());
        store.addContent(valueSerializer);

        if(storeDefinition.hasRetentionPeriod())
            store.addContent(new Element(STORE_RETENTION_POLICY_ELMT).setText(Integer.toString(storeDefinition.getRetentionDays())));

        if(storeDefinition.hasRetentionScanThrottleRate())
            store.addContent(new Element(STORE_RETENTION_SCAN_THROTTLE_RATE_ELMT).setText(Integer.toString(storeDefinition.getRetentionScanThrottleRate())));

        if(storeDefinition.hasMemoryFootprint()) {
            store.addContent(new Element(STORE_MEMORY_FOOTPRINT).setText(Long.toString(storeDefinition.getMemoryFootprintMB())));
        }

        return store;
    }

    private Element viewToElement(StoreDefinition storeDefinition) {
        Element store = new Element(VIEW_ELMT);
        store.addContent(new Element(STORE_NAME_ELMT).setText(storeDefinition.getName()));
        store.addContent(new Element(VIEW_TARGET_ELMT).setText(storeDefinition.getViewTargetStoreName()));
        if(storeDefinition.getDescription() != null)
            store.addContent(new Element(STORE_DESCRIPTION_ELMT).setText(storeDefinition.getDescription()));
        if(storeDefinition.getOwners() != null && storeDefinition.getOwners().size() > 0) {
            String ownersText = StringUtils.join(storeDefinition.getOwners().toArray(), ", ");
            store.addContent(new Element(STORE_OWNERS_ELMT).setText(ownersText));
        }
        if(storeDefinition.getValueTransformation() == null)
            throw new MappingException("View " + storeDefinition.getName()
                                       + " has no defined transformation class.");
        store.addContent(new Element(VIEW_TRANS_ELMT).setText(storeDefinition.getValueTransformation()));
        store.addContent(new Element(STORE_ROUTING_TIER_ELMT).setText(storeDefinition.getRoutingPolicy()
                                                                                     .toDisplay()));
        if(storeDefinition.hasPreferredReads())
            store.addContent(new Element(STORE_PREFERRED_READS_ELMT).setText(Integer.toString(storeDefinition.getPreferredReads())));
        store.addContent(new Element(STORE_REQUIRED_READS_ELMT).setText(Integer.toString(storeDefinition.getRequiredReads())));
        if(storeDefinition.hasPreferredWrites())
            store.addContent(new Element(STORE_PREFERRED_WRITES_ELMT).setText(Integer.toString(storeDefinition.getPreferredWrites())));
        store.addContent(new Element(STORE_REQUIRED_WRITES_ELMT).setText(Integer.toString(storeDefinition.getRequiredWrites())));

        if(storeDefinition.hasZoneCountReads())
            store.addContent(new Element(STORE_ZONE_COUNT_READS).setText(Integer.toString(storeDefinition.getZoneCountReads())));
        if(storeDefinition.hasZoneCountWrites())
            store.addContent(new Element(STORE_ZONE_COUNT_WRITES).setText(Integer.toString(storeDefinition.getZoneCountWrites())));

        Element valueSerializer = new Element(STORE_VALUE_SERIALIZER_ELMT);
        addSerializer(valueSerializer, storeDefinition.getValueSerializer());
        store.addContent(valueSerializer);

        Element transformsSerializer = new Element(STORE_TRANSFORM_SERIALIZER_ELMT);
        if(storeDefinition.getTransformsSerializer() != null) {
            addSerializer(transformsSerializer, storeDefinition.getTransformsSerializer());
            store.addContent(transformsSerializer);
        }

        Element serializerFactory = new Element(VIEW_SERIALIZER_FACTORY_ELMT);
        if(storeDefinition.getSerializerFactory() != null) {
            serializerFactory.setText(storeDefinition.getSerializerFactory());
            store.addContent(serializerFactory);
        }
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

        if(def.hasCompression()) {
            Compression compression = def.getCompression();
            Element compressionElmt = new Element(STORE_COMPRESSION_ELMT);
            Element type = new Element(STORE_COMPRESSION_TYPE_ELMT);
            type.setText(compression.getType());
            compressionElmt.addContent(type);
            String optionsText = compression.getOptions();
            if(optionsText != null) {
                Element options = new Element(STORE_COMPRESSION_OPTIONS_ELMT);
                options.setText(optionsText);
                compressionElmt.addContent(options);
            }
            parent.addContent(compressionElmt);
        }
    }

    public Integer getChildWithDefault(Element elmt, String property, Integer defaultVal) {
        if(elmt.getChildText(property) == null)
            return defaultVal;
        else
            return Integer.parseInt(elmt.getChildText(property));
    }

}
