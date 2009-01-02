package voldemort.xml;

import static voldemort.VoldemortTestConstants.getSimpleStoreDefinitionsXml;

import java.io.StringReader;
import java.util.List;

import junit.framework.TestCase;
import voldemort.VoldemortTestConstants;
import voldemort.store.StoreDefinition;

public class StoreDefinitionMapperTest extends TestCase {

    public void testParsing() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(getSimpleStoreDefinitionsXml()));
        String output = mapper.writeStoreList(storeDefs);
        List<StoreDefinition> found = mapper.readStoreList(new StringReader(output));
        assertEqual(storeDefs, found);
        for(StoreDefinition def : storeDefs) {
            String xml = mapper.writeStore(def);
            StoreDefinition newDef = mapper.readStore(new StringReader(xml));
            assertEquals(def, newDef);
        }
    }

    public void testSingleStore() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getSingleStoreDefinitionsXml()));
        String written = mapper.writeStoreList(storeDefs);
        assertEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    public void testRetentionStore() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getStoreDefinitionsWithRetentionXml()));
        String written = mapper.writeStoreList(storeDefs);
        assertEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    private void assertEqual(List<StoreDefinition> l1, List<StoreDefinition> l2) {
        assertEquals(l1.size(), l2.size());
        for(int i = 0; i < l1.size(); i++) {
            assertEquals(l1.get(i), l2.get(i));
        }
    }

}
