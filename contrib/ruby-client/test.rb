require 'store_client'

s = StoreClient.new("test", [["localhost", "6666"]])
version = s.put("hello", "1")
raise "Invalid result" unless s.get("hello")[0][0] == "1"
s.put("hello", "2", version)
raise "Invalid result" unless s.get("hello")[0][0] == "2"
s.put("hello", "3")
raise "Invalid result" unless s.get("hello")[0][0] == "3"
s.delete("hello")
raise "Invalid result" unless s.get("hello").size == 0
