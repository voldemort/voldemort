package voldemort.cluster;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class NodeTest {

    private static final Random generator = new Random();

    private static final int id = 7;
    private static final String host = "localhost";
    private static final int httpPort = 8080;
    private static final int socketPort = 71;
    private static final int adminPort = 31;
    private static final int zoneId = 43;
    private static final List<Integer> partitions = ImmutableList.of(37, 67, 123);
    private static final int restPort = 23;

    private static final Node node = new Node(id,
                                              host,
                                              httpPort,
                                              socketPort,
                                              adminPort,
                                              zoneId,
                                              partitions,
                                              restPort);

    private Node generateNode(int id) {
        return new Node(id, host, httpPort, socketPort, adminPort, partitions);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_with_null_host() {
        new Node(id, null, httpPort, socketPort, adminPort, zoneId, partitions, restPort);
    }

    /**
     * Unfortunately as of now it throws {@code NullPointerException} instead of
     * {@code IllegalArgumentException}
     */
    @Test(expected = IllegalArgumentException.class)
    public void test_with_null_partitions() {
        new Node(id, host, httpPort, socketPort, adminPort, zoneId, null, restPort);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_with_null_host_and_null_partitions() {
        new Node(id, null, httpPort, socketPort, adminPort, zoneId, null, restPort);
    }

    @Test
    public void testHashCode() {
        assertThat(node.hashCode(), is(id));
    }

    @Test
    public void test_simple_getters() throws URISyntaxException {
        for(int i = 0; i < 100; i++) {
            assertThat(node.getId(), is(id));
            assertThat(node.getHost(), is(host));
            assertThat(node.getHttpPort(), is(httpPort));
            assertThat(node.getSocketPort(), is(node.getSocketPort()));
            assertThat(node.getAdminPort(), is(adminPort));
            assertThat(node.getZoneId(), is(zoneId));
            assertThat(node.getPartitionIds(), equalTo(partitions));
            assertThat(node.getRestPort(), is(restPort));

            assertThat(node.getNumberOfPartitions(), is(partitions.size()));

            assertThat(node.getHttpUrl(), equalTo(new URI("http://" + host + ":" + httpPort)));
            assertThat(node.getSocketUrl(), equalTo(new URI("tcp://" + host + ":" + socketPort)));
        }
    }

    @Test
    public void testToString() {
        final String expected = "Node " + host + " Id:" + id + " in zone " + zoneId
                                + " partitionList:" + partitions;

        for(int i = 0; i < 100; i++) {
            assertThat(node.toString(), equalTo(expected));
        }
    }

    @Test
    public void testGetStateString() {
        final String expected = "Node " + host + " Id:" + id + " in zone " + zoneId
                                + " with admin port " + adminPort + ", socket port " + socketPort
                                + ", and http port " + httpPort;

        for(int i = 0; i < 100; i++) {
            assertThat(node.getStateString(), equalTo(expected));
        }
    }

    @Test
    public void testEqualsObject() {
        for(int i = 0; i < 1000; i++) {
            int id1 = generator.nextInt();
            int id2 = generator.nextInt();

            assertThat(generateNode(id1).equals(null), is(false));
            assertThat(generateNode(id2).equals(new Object()), is(false));

            assertThat(generateNode(id1).equals(generateNode(id2)), is(false));

            assertThat(generateNode(id1).equals(generateNode(id1)), is(true));
            assertThat(generateNode(id2).equals(generateNode(id2)), is(true));
        }
    }

    @Test
    public void testCompareTo() {
        for(int i = 0; i < 1000; i++) {
            Integer id1 = generator.nextInt();
            Integer id2 = generator.nextInt();

            assertThat(generateNode(id1).compareTo(generateNode(id1)), is(id1.compareTo(id1)));
            assertThat(generateNode(id2).compareTo(generateNode(id2)), is(id2.compareTo(id2)));

            assertThat(generateNode(id1).compareTo(generateNode(id2)), is(id1.compareTo(id2)));
            assertThat(generateNode(id2).compareTo(generateNode(id1)), is(id2.compareTo(id1)));
        }
    }

    @Test
    public void testIsEqualState() {
        assertThat(node.isEqualState(node), is(true));
        assertThat(node.isEqualState(new Node(id,
                                              host,
                                              httpPort,
                                              socketPort,
                                              adminPort,
                                              zoneId,
                                              partitions,
                                              id)), is(true));
        assertThat(node.isEqualState(new Node(id,
                                              host.toUpperCase(),
                                              httpPort,
                                              socketPort,
                                              adminPort,
                                              zoneId,
                                              partitions,
                                              id)), is(true));

        assertThat(node.isEqualState(new Node(httpPort,
                                              host,
                                              id,
                                              socketPort,
                                              adminPort,
                                              zoneId,
                                              partitions,
                                              id)), is(false));
        assertThat(node.isEqualState(new Node(id,
                                              host,
                                              socketPort,
                                              httpPort,
                                              adminPort,
                                              zoneId,
                                              partitions,
                                              id)), is(false));
        assertThat(node.isEqualState(new Node(id,
                                              host + " ",
                                              httpPort,
                                              socketPort,
                                              adminPort,
                                              zoneId,
                                              partitions,
                                              id)), is(false));
    }

}
