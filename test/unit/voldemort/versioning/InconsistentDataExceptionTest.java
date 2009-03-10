package voldemort.versioning;

import java.util.List;

import junit.framework.TestCase;

import com.google.common.collect.ImmutableList;

public class InconsistentDataExceptionTest extends TestCase {

    public void testGetVersions() {
        List<?> versions = ImmutableList.of("version1", "version2");
        InconsistentDataException exception = new InconsistentDataException("foo", versions);
        assertEquals(versions, exception.getVersions());
    }
}
