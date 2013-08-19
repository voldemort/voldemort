package voldemort.rest;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.xml.MappingException;
import voldemort.xml.StoreDefinitionsMapper;

public class RestUtils {

    /**
     * Function to serialize the given Vector clock into a string. If something
     * goes wrong, it returns an empty string.
     * 
     * @param vc The Vector clock to serialize
     * @return The string (JSON) version of the specified Vector clock
     */
    public static String getSerializedVectorClock(VectorClock vc) {
        VectorClockWrapper vcWrapper = new VectorClockWrapper(vc);
        ObjectMapper mapper = new ObjectMapper();
        String serializedVC = "";
        try {
            serializedVC = mapper.writeValueAsString(vcWrapper);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return serializedVC;
    }

    public static VectorClock deserializeVectorClock(String serializedVC) {
        VectorClock vc = null;

        if(serializedVC == null) {
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();

        try {
            VectorClockWrapper vcWrapper = mapper.readValue(serializedVC, VectorClockWrapper.class);
            vc = new VectorClock(vcWrapper.getVersions(), vcWrapper.getTimestamp());
        } catch(Exception e) {
            e.printStackTrace();
        }

        return vc;
    }

    /**
     * Function to serialize the given list of Vector clocks into a string. If
     * something goes wrong, it returns an empty string.
     * 
     * @param vectorClocks The Vector clock list to serialize
     * @return The string (JSON) version of the specified Vector clock
     */
    public static String getSerializedVectorClocks(List<VectorClock> vectorClocks) {
        List<VectorClockWrapper> vectorClockWrappers = new ArrayList<VectorClockWrapper>();
        for(VectorClock vc: vectorClocks) {
            vectorClockWrappers.add(new VectorClockWrapper(vc));
        }
        ObjectMapper mapper = new ObjectMapper();
        String serializedVC = "";
        try {
            serializedVC = mapper.writeValueAsString(vectorClockWrappers);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return serializedVC;
    }

    public static List<Version> deserializeVectorClocks(String serializedVC) {
        Set<VectorClockWrapper> vectorClockWrappers = null;
        List<Version> vectorClocks = null;

        if(serializedVC == null) {
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();

        try {
            vectorClockWrappers = mapper.readValue(serializedVC,
                                                   new TypeReference<Set<VectorClockWrapper>>() {});
            if(vectorClockWrappers.size() > 0) {
                vectorClocks = new ArrayList<Version>();
            }
            for(VectorClockWrapper vectorClockWrapper: vectorClockWrappers) {
                vectorClocks.add(new VectorClock(vectorClockWrapper.getVersions(),
                                                 vectorClockWrapper.getTimestamp()));
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        return vectorClocks;
    }

    /**
     * Given a storedefinition, constructs the xml string to be sent out in
     * response to a "schemata" fetch request
     * 
     * @param storeDefinition
     * @return serialized store definition 
     */
    public static String constructSerializerInfoXml(StoreDefinition storeDefinition) {
        Element store = new Element(StoreDefinitionsMapper.STORE_ELMT);
        store.addContent(new Element(StoreDefinitionsMapper.STORE_NAME_ELMT).setText(storeDefinition.getName()));
        Element keySerializer = new Element(StoreDefinitionsMapper.STORE_KEY_SERIALIZER_ELMT);
        StoreDefinitionsMapper.addSerializer(keySerializer, storeDefinition.getKeySerializer());
        store.addContent(keySerializer);

        Element valueSerializer = new Element(StoreDefinitionsMapper.STORE_VALUE_SERIALIZER_ELMT);
        StoreDefinitionsMapper.addSerializer(valueSerializer, storeDefinition.getValueSerializer());
        store.addContent(valueSerializer);

        XMLOutputter serializer = new XMLOutputter(Format.getPrettyFormat());
        return serializer.outputString(store);
    }

    /**
     * Given an xml string containing the store's serialization information,
     * obtains the key serializer definition
     * 
     * @param serializerInfoXml
     * @return key serializer definition
     */
    public static SerializerDefinition parseKeySerializerDefinition(String serializerInfoXml) {
        return parseSerializerDefinition(serializerInfoXml,
                                         StoreDefinitionsMapper.STORE_KEY_SERIALIZER_ELMT);
    }

    /**
     * Given an xml string containing the store's serialization information,
     * obtains the value serializer definition
     * 
     * @param serializerInfoXml
     * @return serializer definition
     */
    public static SerializerDefinition parseValueSerializerDefinition(String serializerInfoXml) {
        return parseSerializerDefinition(serializerInfoXml,
                                         StoreDefinitionsMapper.STORE_VALUE_SERIALIZER_ELMT);
    }

    private static SerializerDefinition parseSerializerDefinition(String serializerInfoXml,
                                                                  String elementName) {
        SAXBuilder builder = new SAXBuilder();
        try {
            Document doc = builder.build(new StringReader(serializerInfoXml));
            Element root = doc.getRootElement();
            Element serializerElement = root.getChild(elementName);
            return StoreDefinitionsMapper.readSerializer(serializerElement);
        } catch(JDOMException e) {
            throw new MappingException(e);
        } catch(IOException e) {
            throw new MappingException(e);
        }
    }

    public static String encodeVoldemortKey(byte[] keyBytes) {
        String base64Key = Base64.encodeBase64URLSafeString(keyBytes);
        return base64Key;
    }

    public static byte[] decodeVoldemortKey(String base64Key) {
        byte[] keyBytes = Base64.decodeBase64(base64Key);
        return keyBytes;
    }

}
