package voldemort.versioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import voldemort.TestUtils;

public class VectorClockInconsistencyResolverTest extends TestCase {
	
	private InconsistencyResolver<Versioned<String>> resolver;
	private Versioned<String> later;
	private Versioned<String> prior;
	private Versioned<String> current;
	private Versioned<String> concurrent;
	
	public void setUp() {
		resolver = new VectorClockInconsistencyResolver<String>();
		current = getVersioned(1,1,2,3);
		prior = getVersioned(1,2,3);
		concurrent = getVersioned(1,2,3,3);
		later = getVersioned(1,1,2,2,3);
	}
	
	private Versioned<String> getVersioned(int...nodes) {
		return new Versioned<String>("my-value", TestUtils.getClock(nodes));
	}
	
	public void testEmptyList() {
		assertEquals(0, resolver.resolveConflicts(new ArrayList<Versioned<String>>()).size());
	}
	
	@SuppressWarnings("unchecked")
	public void testDuplicatesResolve() {
		assertEquals(2, resolver.resolveConflicts(Arrays.asList(concurrent, current, current, concurrent, current)).size());
	}
	
	@SuppressWarnings("unchecked")
	public void testResolveNormal() {
		assertEquals(later, resolver.resolveConflicts(Arrays.asList(current, prior, later)).get(0));
		assertEquals(later, resolver.resolveConflicts(Arrays.asList(prior, current, later)).get(0));
		assertEquals(later, resolver.resolveConflicts(Arrays.asList(later, current, prior)).get(0));
	}
	
	@SuppressWarnings("unchecked")
	public void testResolveConcurrent() {
		List<Versioned<String>> resolved = resolver.resolveConflicts(Arrays.asList(current, concurrent, prior));
		assertEquals(2, resolved.size());
		assertTrue("Version not found", resolved.contains(current));
		assertTrue("Version not found", resolved.contains(concurrent));
	}
}
