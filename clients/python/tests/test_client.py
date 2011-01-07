# Tests of the client code.
#
# To run these tests, you must have a local Voldemort server running using the configuration files
# in tests/voldemort_config.

import unittest
import datetime

from voldemort import StoreClient, VoldemortException

def _vector_clock_equal(clock1, clock2):
    """
    Compares two vector clocks, ignoring the timestamp field, which may be skewed.
    """

    clock1_entries = dict((entry.node_id, entry.version) for entry in clock1.entries)
    clock2_entries = dict((entry.node_id, entry.version) for entry in clock2.entries)
    return clock1_entries == clock2_entries

class VoldemortClientTest(unittest.TestCase):
    def _reinit_raw_client(self):
        s = StoreClient('test', [('localhost', 6666)])
        for k in ['a', 'b', 'c']:
            s.delete(k)
        return s

    def _reinit_json_client(self):
        s = StoreClient('json_test', [('localhost', 6666)])
        for k in [1, 2, 3]:
            s.delete(k)
        return s

    def test_raw_get(self):
        """
        Tests basic puts/gets in raw (non-serialized) mode.
        """

        s = self._reinit_raw_client()

        s.put('a', '1')
        resp = s.get('a')
        self.assertEquals(len(resp), 1)
        self.assertEquals(len(resp[0]), 2)
        self.assertEquals(resp[0][0], '1')

        s.put('b', '2')
        resp = s.get('b')
        self.assertEquals(len(resp), 1)
        self.assertEquals(len(resp[0]), 2)
        self.assertEquals(resp[0][0], '2')


    def test_raw_get_all(self):
        """
        Tests the get_all() method in raw mode.
        """

        s = self._reinit_raw_client()

        pairs = [('a', '1'), ('b', '2'), ('c', '3')]
        for k, v in pairs:
            s.put(k, v)

        resp = s.get_all([k for k, v in pairs])
        self.assertEquals(len(resp), len(pairs))

        for k, v in pairs:
            self.assertTrue(k in resp)
            self.assertEquals(len(resp[k]), 1)
            self.assertEquals(len(resp[k][0]), 2)

            self.assertEquals(resp[k][0][0], v)


    def test_raw_versions(self):
        """
        Tests the put_maybe() method in raw mode.
        """

        s = self._reinit_raw_client()

        v1 = s.put('a', '1')
        self.assertTrue(v1 is not None)

        v2 = s.put('a', '2')
        self.assertTrue(v2 is not None)
        self.assertFalse(_vector_clock_equal(v2, v1))

        v3 = s.put('a', '3')
        self.assertTrue(v3 is not None)
        self.assertFalse(_vector_clock_equal(v3, v1))
        self.assertFalse(_vector_clock_equal(v3, v2))

        resp = s.get('a')
        self.assertEquals(resp[0][0], '3')

        # put() should fail because v2 is not the current version
        self.assertRaises(VoldemortException, s.put, 'a', '4', version=v2)

        # maybe_put() won't raise an exception, but will return None
        v4 = s.maybe_put('a', '4', version=v2)
        self.assertTrue(v4 is None)

        # this put() should succeed
        v4 = s.put('a', '4', version=v3)
        self.assertTrue(v4 is not None)
        self.assertFalse(_vector_clock_equal(v4, v1))
        self.assertFalse(_vector_clock_equal(v4, v2))
        self.assertFalse(_vector_clock_equal(v4, v2))

        # and this maybe_put() should not return None
        v5 = s.maybe_put('a', '5', version=v4)
        self.assertTrue(v5 is not None)
        self.assertFalse(_vector_clock_equal(v5, v1))
        self.assertFalse(_vector_clock_equal(v5, v2))
        self.assertFalse(_vector_clock_equal(v5, v3))
        self.assertFalse(_vector_clock_equal(v5, v4))

        # the value at the latest version should be "5"
        resp = s.get('a')
        self.assertEquals(resp[0][0], '5')
        self.assertTrue(_vector_clock_equal(resp[0][1], v5))

        # deleting old versions should have no effect
        s.delete('a', version=v3)
        resp = s.get('a')
        self.assertEquals(len(resp), 1)
        self.assertEquals(resp[0][0], '5')
        self.assertTrue(_vector_clock_equal(resp[0][1], v5))

        # deleting the current version should erase the entry
        s.delete('a', version=v5)
        resp = s.get('a')
        self.assertEquals(resp, [])

    val1 = {
        'a': 0.25,
        'b': [1,2,3],
        'c': u'foo',
        'd': { 'foo': True,
               'bar': datetime.datetime(2010, 11, 24, 20, 8, 7, 155000)
               }
        }

    val2 = {
        'a': 4.0,
        'b': [5,6],
        'c': u'bar',
        'd': { 'foo': None,
               'bar': datetime.datetime(2003, 5, 5, 1, 23, 45, 678000)
               }
        }

    val3 = {
        'a': 8.0,
        'b': [],
        'c': u'',
        'd': None
        }

    val4 = {
        'a': 4.0,
        'b': [5,6],
        'c': 'bar',
        'd': { 'foo': True,
               'bar': datetime.datetime(2003, 5, 5, 1, 23, 45, 678123)
               }
        }

    def test_json_get(self):
        """
        Tests the JSON serialization with put()/get()
        """

        s = self._reinit_json_client()

        s.put(1, self.val1)
        resp = s.get(1)
        self.assertEquals(len(resp), 1)
        self.assertEquals(len(resp[0]), 2)
        self.assertEquals(resp[0][0], self.val1)

        s.put(2, self.val2)
        resp = s.get(2)
        self.assertEquals(len(resp), 1)
        self.assertEquals(len(resp[0]), 2)
        self.assertEquals(resp[0][0], self.val2)

    def test_json_get_all(self):
        """
        Tests JSON serialized get_all()
        """
        s = self._reinit_json_client()

        pairs = [(1, self.val1), (2, self.val2), (3, self.val3)]
        for k, v in pairs:
            s.put(k, v)

        resp = s.get_all([k for k, v in pairs])
        self.assertEquals(len(resp), len(pairs))

        for k, v in pairs:
            self.assertTrue(k in resp)
            self.assertEquals(len(resp[k]), 1)
            self.assertEquals(len(resp[k][0]), 2)

            self.assertEquals(resp[k][0][0], v)

    def test_json_mismatches(self):
        """
        Sometimes the result we get out of Voldemort is a little different than what
        went in, but it's not always a problem.
        """

        s = self._reinit_json_client()

        s.put(1, self.val4)
        resp = s.get(1)
        self.assertEquals(len(resp), 1)
        self.assertEquals(len(resp[0]), 2)

        output = resp[0][0]
        # the input and output won't be the same
        self.assertNotEquals(output, self.val4)

        # the float should have survived the trip
        self.assertEquals(output['a'], self.val4['a'])

        # as should the list
        self.assertEquals(output['b'], self.val4['b'])

        # the string is now a unicode, but it should still compare equal to the original
        self.assertTrue(isinstance(output['c'], unicode))
        self.assertEquals(output['c'], self.val4['c'])

        # the boolean should be the same
        self.assertEquals(output['d']['foo'], self.val4['d']['foo'])

        # but the date gets truncated:
        self.assertNotEquals(output['d']['bar'], self.val4['d']['bar'])

        # the difference should be small
        td = self.val4['d']['bar'] - output['d']['bar']
        self.assertEquals(td.days, 0)
        self.assertEquals(td.seconds, 0)
        self.assertEquals(td.microseconds, 123)

    def test_raw_versions(self):
        """
        Tests versioning in JSON mode.
        """

        s = self._reinit_json_client()

        v1 = s.put(1, self.val4)
        self.assertTrue(v1 is not None)

        v2 = s.put(1, self.val3)
        self.assertTrue(v2 is not None)
        self.assertFalse(_vector_clock_equal(v2, v1))

        resp = s.get(1)
        self.assertEquals(resp[0][0], self.val3)
        self.assertTrue(_vector_clock_equal(resp[0][1], v2))

        # put() should fail because v1 is not the current version
        self.assertRaises(VoldemortException, s.put, 1, self.val2, version=v1)

        # maybe_put() won't raise an exception, but will return None
        v3 = s.maybe_put(1, self.val2, version=v1)
        self.assertTrue(v3 is None)

        # this put() should succeed
        v3 = s.put(1, self.val2, version=v2)
        self.assertTrue(v3 is not None)
        self.assertFalse(_vector_clock_equal(v3, v1))
        self.assertFalse(_vector_clock_equal(v3, v2))

        # and this maybe_put() should not return None
        v4 = s.maybe_put(1, self.val1, version=v3)
        self.assertTrue(v4 is not None)
        self.assertFalse(_vector_clock_equal(v4, v1))
        self.assertFalse(_vector_clock_equal(v4, v2))
        self.assertFalse(_vector_clock_equal(v4, v3))

        # the value at the latest version should be val1 at version v4
        resp = s.get(1)
        self.assertEquals(resp[0][0], self.val1)
        self.assertTrue(_vector_clock_equal(resp[0][1], v4))

        # deleting old versions should have no effect
        s.delete(1, version=v2)
        resp = s.get(1)
        self.assertEquals(len(resp), 1)
        self.assertEquals(resp[0][0], self.val1)
        self.assertTrue(_vector_clock_equal(resp[0][1], v4))

        # deleting the current version should erase the entry
        s.delete(1, version=v4)
        resp = s.get(1)
        self.assertEquals(resp, [])

