require File.dirname(__FILE__) + '/spec_helper'

describe Connection do

  before(:each) do
    @connection = Connection.new("test", "localhost:6666")
  end

  describe "default methods" do

    it "should support connect" do
      @connection.should respond_to(:connect)
    end

    it "should support reconnect" do
      @connection.should respond_to(:reconnect)
    end

    it "should support disconnect" do
      @connection.should respond_to(:disconnect)
    end

    it "should parse nodes from xml" do
      @connection.should respond_to(:parse_nodes_from)
      xml = "<cluster>\r\n  <name>mycluster</name>\r\n  <server>\r\n    <id>0</id>\r\n    <host>localhost</host>\r\n    <http-port>8081</http-port>\r\n    <socket-port>6666</socket-port>\r\n    <admin-port>6667</admin-port>\r\n    <partitions>0, 1</partitions>\r\n  </server>\r\n</cluster>"
      doc = Nokogiri::XML(xml)
      nodes = @connection.parse_nodes_from(doc)
      nodes.first.host.should eql("localhost")
      nodes.first.port.should eql("6666")
      nodes.length.should eql(1)
    end

    it "should tell to wich node is connected to" do
      @connection.should respond_to(:connected_node)
      node = mock(VoldemortNode)
      node.stub!(:host).and_return("localhost")
      node.stub!(:port).and_return(6666)
      @connection.nodes.stub!(:sort_by).and_return([node])
      @connection.stub!(:connect_to).and_return(true)
      @connection.connect_to_random_node
      @connection.connected_node.should eql(node)
    end

    it "should use protobuf by default" do
      @connection.protocol_version.should eql("pb0")
    end

    it "should use the hosts specified" do
      connection = Connection.new("test", "localhost:6666")
      connection.hosts.should eql("localhost:6666")
      connection.nodes.length.should eql(1)
      connection2 = Connection.new("test", ["localhost:6666", "localhost:7777"])
      connection2.hosts.should eql(["localhost:6666", "localhost:7777"])
      connection2.nodes.length.should eql(2)
    end
  end

  describe "rebalance nodes by evaluating number of requests" do
    
    it "should have a request_count and request_limit_per_node per node connection" do
      @connection.should respond_to(:request_count)
      @connection.should respond_to(:request_limit_per_node)
    end
    
    it "should tell if the request limit per node was reached" do
      @connection.request_count = 0
      @connection.request_limit_per_node = 10
      @connection.rebalance_connection?.should eql(false)
      @connection.request_count = 11
      @connection.request_limit_per_node = 10
      @connection.rebalance_connection?.should eql(true)
    end
    
    it "should reconnect every N number of requests" do
      @connection.should_receive(:rebalance_connection?).and_return(true)
      @connection.should_receive(:reconnect).and_return(true)
      @connection.rebalance_connection_if_needed
    end
    
    it "should not reconnect if it haven't reached the limit of requests" do
      @connection.should_receive(:rebalance_connection?).and_return(false)
      @connection.should_not_receive(:reconnect).and_return(false)
      @connection.rebalance_connection_if_needed
    end
    
    it "should rebalance if needed when calling get, get_all or put" do
      @connection.should_receive(:rebalance_connection_if_needed).exactly(3).times.and_return(true)
      @connection.stub!(:get_from).and_return(true)
      @connection.stub!(:get_all_from).and_return(true)
      @connection.stub!(:put_from).and_return(true)
      @connection.stub!(:delete_from).and_return(true)
      @connection.get("value")
      @connection.put("value", "value")
      @connection.get_all(["key1", "key2"])
      @connection.delete("key")
    end
  end
end