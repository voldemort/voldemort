package voldemort.utils;

import org.junit.Assert;
import org.junit.Test;
import voldemort.VoldemortException;

import java.io.IOException;
import java.net.ConnectException;

public class ExceptionUtilsTest {
    @Test
    public void testRecursiveClassEquals() {
        Assert.assertTrue(
                "recursiveClassEquals should recognize the top-level exception.",
                ExceptionUtils.recursiveClassEquals(
                        new VoldemortException("blah", new IOException("bleh")),
                        VoldemortException.class));

        Assert.assertFalse(
                "recursiveClassEquals should NOT recognize the exception.",
                ExceptionUtils.recursiveClassEquals(
                        new VoldemortException("blah", new IOException("bleh")),
                        NullPointerException.class));

        Assert.assertTrue(
                "recursiveClassEquals should recognize a nested exception.",
                ExceptionUtils.recursiveClassEquals(
                        new VoldemortException("blah", new IOException("bleh")),
                        IOException.class));

        Assert.assertTrue(
                "recursiveClassEquals should recognize a nested exception which is the child of the class we're looking for.",
                ExceptionUtils.recursiveClassEquals(
                        new VoldemortException("blah", new ConnectException("bleh")),
                        IOException.class));

        Assert.assertFalse(
                "recursiveClassEquals should NOT recognize a nested exception which is the parent of the class we're looking for.",
                ExceptionUtils.recursiveClassEquals(
                        new VoldemortException("blah", new IOException("bleh")),
                        ConnectException.class));
    }
}
