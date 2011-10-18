package voldemort.utils;


import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import voldemort.MockTime;

import java.util.concurrent.Callable;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CachedCallableTest {

    private static final long CACHE_TTL_MS = 1000;
    private static final long CALL_RESULT = 0xdeaddeadl;

    @Mock
    private Callable<Long> inner;

    private CachedCallable<Long> cachedCallable;
    private MockTime mockTime;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(inner.call()).thenReturn(CALL_RESULT);
        mockTime = new MockTime();
        cachedCallable = new CachedCallable<Long>(inner,
                                                  CACHE_TTL_MS,
                                                  mockTime);
    }

    @Test
    public void testCall() throws Exception {
        assertEquals((long) cachedCallable.call(), CALL_RESULT);
    }

    @Test
    public void testCaching() throws Exception {
        cachedCallable.call();
        cachedCallable.call();
        verify(inner, times(1)).call();
    }

    @Test
    public void testTtl() throws Exception {
        cachedCallable.call();
        cachedCallable.call();
        verify(inner, times(1)).call();
        mockTime.addMilliseconds(1001);
        when(inner.call()).thenReturn(CALL_RESULT + 1l);
        assertEquals((long) cachedCallable.call(), CALL_RESULT + 1l);
        verify(inner, times(2)).call();
    }
}
