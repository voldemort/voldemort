import httplib

def get(host, path, port = 80):
	con = httplib.HTTPConnection(host, port)
	con.request('GET', path)
	response = con.getresponse()
	contents = response.read()
	con.close()
	return response.status, contents

def post(host, path, data, port = 80):
	con = httplib.HTTPConnection(host, port)
	con.request('POST', path, data)
	response = con.getresponse()
	contents = response.read()
	con.close()
	return response.status, contents

def put(host, path, data, port = 80):
	con = httplib.HTTPConnection(host, port)
	con.request('PUT', path, data)
	response = con.getresponse()
	contents = response.read()
	con.close()
	return response.status, contents

def delete(host, path, port = 80):
	con = httplib.HTTPConnection(host, port)
	con.request('DELETE', path)
	response = con.getresponse()
	contents = response.read()
	con.close()
	return response.status, contents

def options(host, path, port = 80):
	con = httplib.HTTPConnection(host, port)
	con.request('OPTIONS', path)
	response = con.getresponse()
	contents = response.read()
	con.close()
	return response.status, contents
