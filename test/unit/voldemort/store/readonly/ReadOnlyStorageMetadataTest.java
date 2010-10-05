package voldemort.store.readonly;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

import org.junit.Test;

import voldemort.TestUtils;

import com.google.common.collect.Maps;

public class ReadOnlyStorageMetadataTest {

    @Test
    public void testToJson() throws IOException {
        Map<String, Object> someProps = Maps.newHashMap();

        // Test empty
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata(someProps);
        String jsonString = metadata.toJsonString();
        ReadOnlyStorageMetadata metadata2 = new ReadOnlyStorageMetadata(jsonString);
        assertEquals(metadata, metadata2);

        // Test with null
        metadata.add(null, null);
        someProps.put(null, null);
        metadata2 = new ReadOnlyStorageMetadata(someProps);
        assertEquals(metadata, metadata2);

        // Test with some values
        metadata.add("key1", "value1");
        metadata.add("key2", "value2");
        metadata.add("key3", "value3");
        metadata.remove("key2");
        someProps.put("key3", "value3");
        someProps.put("key1", "value1");
        metadata2 = new ReadOnlyStorageMetadata(someProps);
        ReadOnlyStorageMetadata metadata3 = new ReadOnlyStorageMetadata(metadata.toJsonString());
        assertEquals(metadata, metadata2);
        assertEquals(metadata, metadata3);

        // Test not equal
        someProps.put("key1", "value123");
        metadata2 = new ReadOnlyStorageMetadata(someProps);
        assertFalse(metadata.equals(metadata2));

        // Write to file
        File tempFile = new File(TestUtils.createTempDir(), TestUtils.randomLetters(5));
        Writer output = new BufferedWriter(new FileWriter(tempFile));
        output.write(metadata3.toJsonString());
        output.close();

        ReadOnlyStorageMetadata metadata4 = new ReadOnlyStorageMetadata(tempFile);
        assertEquals(metadata4, metadata3);

    }
}
