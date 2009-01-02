import rest

## test creating and deleting a service
print rest.delete('localhost','/myservice', 8080)
print rest.get('localhost','/myservice', 8080)
print rest.put('localhost', '/myservice', 8080)
print rest.get('localhost','/myservice', 8080)
print rest.delete('localhost','/myservice', 8080)