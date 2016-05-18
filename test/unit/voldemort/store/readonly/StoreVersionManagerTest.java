package voldemort.store.readonly;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.store.PersistenceFailureException;

import java.io.File;

/**
 * Basic tests for {@link StoreVersionManager}
 */
public class StoreVersionManagerTest extends TestCase {
    private File rootDir, version0, version1;
    private StoreVersionManager storeVersionManager;
    @Before
    public void setUp() {
        rootDir = TestUtils.createTempDir();
        version0 = new File(rootDir, "version-0");
        version1 = new File(rootDir, "version-1");
        Assert.assertTrue("Failed to create version directory!", version0.mkdir());
        Assert.assertTrue("Failed to create version directory!", version1.mkdir());
        storeVersionManager = new StoreVersionManager(rootDir, null);
        storeVersionManager.syncInternalStateFromFileSystem(false);
    }

    @Test
    public void testDisableStoreVersion() {
        // Validate that state got synced properly from the filesystem
        Assert.assertFalse("Did not expect to have any store version disabled.",
                           storeVersionManager.hasAnyDisabledVersion());

        // Disable a store version
        storeVersionManager.disableStoreVersion(0);

        // Validate that the in-memory state changed accordingly
        Assert.assertTrue("Expected in-memory state to have some store version disabled.",
                           storeVersionManager.hasAnyDisabledVersion());
        Assert.assertTrue("Expected in-memory state to have store version 1 enabled.",
                          storeVersionManager.isCurrentVersionEnabled());

        // Verify that another instance of StoreVersionManager, freshly synced from disk, also has proper state.
        StoreVersionManager storeVersionManager2 = new StoreVersionManager(rootDir, null);
        storeVersionManager2.syncInternalStateFromFileSystem(false);
        Assert.assertTrue("Expected persistent state to have some store version disabled.",
                          storeVersionManager2.hasAnyDisabledVersion());
        Assert.assertTrue("Expected persistent state to have store version 1 enabled.",
                          storeVersionManager2.isCurrentVersionEnabled());
    }

    @Test
    public void testDisableStoreVersionOnReadOnlyStorage() {
        // Validate that state got synced properly from the filesystem
        Assert.assertFalse("Did not expect to have any store version disabled.",
                           storeVersionManager.hasAnyDisabledVersion());

        // Simulate the filesystem becoming read-only underneath us
        version0.setReadOnly();

        // Disable a store version
        try {
            storeVersionManager.disableStoreVersion(0);
            Assert.fail("Did not get a PersistenceFailureException when trying to disableStoreVersion on a read-only filesystem.");
        } catch (PersistenceFailureException e) {
            // expected
        }

        // Validate that the in-memory state changed accordingly
        Assert.assertTrue("Expected in-memory state to have some store version disabled.",
                          storeVersionManager.hasAnyDisabledVersion());
        Assert.assertTrue("Expected in-memory state to have store version 1 enabled.",
                          storeVersionManager.isCurrentVersionEnabled());

        // Verify that another instance of StoreVersionManager, freshly synced from disk, does NOT have proper state.
        StoreVersionManager storeVersionManager2 = new StoreVersionManager(rootDir, null);
        storeVersionManager2.syncInternalStateFromFileSystem(false);
        Assert.assertFalse("Expected persistent state to have no version disabled.",
                          storeVersionManager2.hasAnyDisabledVersion());
    }

}
