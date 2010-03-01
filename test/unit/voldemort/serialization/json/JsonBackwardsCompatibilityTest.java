package voldemort.serialization.json;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;

import voldemort.utils.Utils;

import com.google.common.collect.ImmutableList;

/**
 * A test that the json serialization remains compatible with older serialized
 * data
 * 
 * 
 */
public class JsonBackwardsCompatibilityTest extends TestCase {

    private static JsonTypeDefinition def = JsonTypeDefinition.fromJson("[{"
                                                                        + "'int8':'int8',"
                                                                        + "'int16':'int16',"
                                                                        + "'int32':'int32', "
                                                                        + "'int64':'int64',"
                                                                        + "'float32':'float32',"
                                                                        + "'float64':'float64',"
                                                                        + "'string':'string',"
                                                                        + "'list_of_int':['int32'],"
                                                                        + "'d':'date'," + "}]");

    private static List<Map<String, Object>> expected = new ArrayList<Map<String, Object>>();

    static {
        for(int i = -50; i < 50; i++) {
            String string = Integer.toString(i);
            expected.add(makeMapData((byte) i,
                                     (short) i,
                                     i,
                                     (long) i,
                                     (float) i,
                                     (double) i,
                                     string,
                                     ImmutableList.of(i),
                                     new Date(i)));
        }
        expected.add(makeMapData(null, null, null, null, null, null, null, null, null));
    }

    public void testBinaryCompatibility() throws Exception {
        String compatFileName = "/voldemort/serialization/json/compatibility.dat";
        InputStream s = JsonBackwardsCompatibilityTest.class.getResourceAsStream(compatFileName);
        assertNotNull("Did not find file " + compatFileName + " on the class path.", s);
        byte[] bytes = IOUtils.toByteArray(s);
        Object found = new JsonTypeSerializer(def, true).toObject(bytes);
        assertEquals(expected, found);
    }

    private static Map<String, Object> makeMapData(Byte b,
                                                   Short s,
                                                   Integer i,
                                                   Long l,
                                                   Float f,
                                                   Double d,
                                                   String string,
                                                   List<Integer> loi,
                                                   Date date) {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("int8", b);
        m.put("int16", s);
        m.put("int32", i);
        m.put("int64", l);
        m.put("float32", f);
        m.put("float64", d);
        m.put("string", string);
        m.put("list_of_int", loi);
        m.put("d", date);
        return m;
    }

    /*
     * This main method generates data that is stored and used for compatibility
     * testing. Do not regenerate the data after a code change as that would
     * defeat the purpose of a test like this.
     */
    public static void main(String[] args) throws Exception {
        if(args.length != 1)
            Utils.croak("USAGE: java TestCompatibility outputfilename");
        FileOutputStream output = new FileOutputStream(args[0]);
        output.write(new JsonTypeSerializer(def, true).toBytes(expected));
        output.close();
    }

}
