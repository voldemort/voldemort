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


import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.*;
import junit.framework.TestCase;
import voldemort.VoldemortTestConstants;
import voldemort.store.StoreDefinition;

public class StoreDefinitionMapperTest extends TestCase {

    public void testParsing() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getSimpleStoreDefinitionsXml()));
        String output = mapper.writeStoreList(storeDefs);
        List<StoreDefinition> found = mapper.readStoreList(new StringReader(output));
        checkEquals(storeDefs, found);
        for(StoreDefinition def: storeDefs) {
            String xml = mapper.writeStore(def);
            StoreDefinition newDef = mapper.readStore(new StringReader(xml)).get(0);
            assertEquals(def, newDef);
        }
    }

    public void testSingleStore() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getSingleStoreDefinitionsXml()));
        String written = mapper.writeStoreList(storeDefs);
        assertEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    public void testNoVersionStore() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getNoVersionStoreDefinitionsXml()));
        String written = mapper.writeStoreList(storeDefs);
        assertEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    public void testSingleStoreWithZones() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getSingleStoreWithZonesXml()));
        String written = mapper.writeStoreList(storeDefs);
        assertEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    public void testStoreNameExpansion() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getStoreWithExpandingName()));
        assertEquals(2, storeDefs.size());
        assertEquals("prefix.something.suffix", storeDefs.get(0).getName());
        assertEquals("prefix.somethingElse.suffix", storeDefs.get(1).getName());
        String written = mapper.writeStoreList(storeDefs);
        assertEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    public void testTwoVersionsOnKeyFails() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        try {
            mapper.readStoreList(new StringReader(VoldemortTestConstants.getStoreWithTwoKeyVersions()));
            fail("There are multiple versions on the key serializer, which should not be allowed.");
        } catch(MappingException e) {
            // this is good
        }
    }

    public void testRetentionStore() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getStoreDefinitionsWithRetentionXml()));
        String written = mapper.writeStoreList(storeDefs);
        assertEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    public void testCompressedStore() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getCompressedStoreDefinitionsXml()));
        String written = mapper.writeStoreList(storeDefs);
        assertEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    public void testView() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getViewStoreDefinitionXml()));
        String written = mapper.writeStoreList(storeDefs);
        checkEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    public void testWildcardViewAndStoreNameExpansion() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getWildcardViewStoreDefinitionXml()));
        Set<String> expectedStores = Sets.newHashSet("elephants.something", "elephants.somethingElse", "elephants.somethingElseAgain",
                "lions.something", "lions.somethingElse",
                "donkeys");
        Map<String, String> expectedViewsToStores = Maps.newHashMap(ImmutableMap.of(
                "elephants-fixed-view", "elephants.something",
                "elephants-something-view", "elephants.something",
                "elephants-somethingElse-view", "elephants.somethingElse",
                "elephants-somethingElseAgain-view", "elephants.somethingElseAgain"));
        expectedViewsToStores.putAll(ImmutableMap.of(
                "lions-view-something", "lions.something",
                "lions-view-somethingElse", "lions.somethingElse"));
        for (StoreDefinition store : storeDefs) {
            if (!store.isView()) {
                assertTrue("Unexpected store read from XML: " + store.getName(), expectedStores.remove(store.getName()));
            } else {
                String expectedStore = expectedViewsToStores.get(store.getName());
                assertEquals("Unexpected view read from XML: view name " + store.getName() + " is of store " + store.getViewTargetStoreName(),
                        expectedStore, store.getViewTargetStoreName());
                expectedViewsToStores.remove(store.getName());
            }
        }
        assertTrue("Didn't find expected stores: " + Iterables.toString(expectedStores), expectedStores.isEmpty());
        assertTrue("Didn't find expected views: " + Iterables.toString(expectedViewsToStores.keySet()),
                expectedViewsToStores.isEmpty());

        String written = mapper.writeStoreList(storeDefs);
        checkEquals(storeDefs, mapper.readStoreList(new StringReader(written)));
    }

    public void testExpandStoreNames() {
        checkStringListsEqual(ImmutableList.of("aSingleFixedStoreName"), StoreDefinitionsMapper.expandStoreNames("aSingleFixedStoreName"));
        checkStringListsEqual(ImmutableList.of("first store","second store"), StoreDefinitionsMapper.expandStoreNames("{first store, second store}"));
        checkStringListsEqual(ImmutableList.of("prefix.first store","prefix.second store"), StoreDefinitionsMapper.expandStoreNames("prefix.{first store, second store}"));
        checkStringListsEqual(ImmutableList.of("first store.suffix","second store.suffix"), StoreDefinitionsMapper.expandStoreNames("{first store, second store}.suffix"));
        checkStringListsEqual(ImmutableList.of("prefix.first store.suffix","prefix.second store.suffix"), StoreDefinitionsMapper.expandStoreNames("prefix.{first store, second store}.suffix"));
        checkStringListsEqual(ImmutableList.of("prefix.first store.suffix"), StoreDefinitionsMapper.expandStoreNames("prefix.{\tfirst store }.suffix"));
    }

    private void checkEquals(List<StoreDefinition> l1, List<StoreDefinition> l2) {
        assertEquals(l1.size(), l2.size());
        for(int i = 0; i < l1.size(); i++)
            assertEquals(l1.get(i), l2.get(i));
    }

    private void checkStringListsEqual(List<String> l1, List<String> l2) {
        assertEquals(l1.size(), l2.size());
        for(int i = 0; i < l1.size(); i++)
            assertEquals(l1.get(i), l2.get(i));
    }

}
