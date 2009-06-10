import logging
import time
from voldemort import StoreClient

if __name__ == '__main__':
	
	logging.basicConfig(level=logging.INFO,)
	
	## some random tests
	s = StoreClient('test', [('localhost', 6666)])
	version = s.put("hello", "1")
	assert s.get("hello")[0][0] == "1"
	s.put("hello", "2", version)
	assert s.get("hello")[0][0] == "2"
	s.put("hello", "3")
	assert s.get("hello")[0][0] == "3"
	s.delete("hello")
	assert len(s.get("hello")) == 0
	
	## test get_all
	pairs = [("a1", "1"), ("a2", "2"), ("a3", "3"), ("a4", "4")]
	for k, v in pairs:
		s.put(k, v)
	vals = s.get_all([k for k, v in pairs])
	for k, v in pairs:
		assert vals[k][0][0] == v 
	
	requests = 10000
	
	## Time get requests
	s.put("hello", "world")
	start = time.time()
	for i in xrange(requests):
		s.get('hello')
	print requests/(time.time() - start), ' get requests per second'
	
	## Time put requests
	version = s.put('abc', 'def')
	start = time.time()
	for i in xrange(requests):
		version = s.put('abc', 'def', version)
	print requests/(time.time() - start), ' put requests per second'
	
	## Time get_all requests
	keys = [k for k,v in pairs]
	start = time.time()
	for i in xrange(requests):
		vals = s.get_all(keys)
	print requests/(time.time() - start), ' get_all requests per second'
	
	## Time delete requests
	version = None
	for i in xrange(requests):
		version = s.put(str(i), str(i))
	start = time.time()
	for i in xrange(requests):
		vals = s.delete(str(i), version)
	print requests/(time.time() - start), ' delete requests per second'