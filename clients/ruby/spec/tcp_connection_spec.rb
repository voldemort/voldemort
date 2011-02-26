require File.dirname(__FILE__) + '/spec_helper'

describe TCPConnection do

  before(:each) do
    @connection = TCPConnection.new("test", "localhost:6666")
  end

  describe "connection mechanism" do

    it "should connect to a specified host" do
      @connection.should respond_to(:connect_to)
      mock_socket = mock(TCPSocket)
      TCPSocket.should_receive(:open).and_return(mock_socket)
      @connection.should_receive(:send_protocol_version).and_return(true)
      @connection.should_receive(:protocol_handshake_ok?).and_return(true)
      @connection.connect_to("localhost", 6666).should eql(mock_socket)
    end

    it "should send the protocol" do
      @connection.should respond_to(:send_protocol_version)
      mock_socket = mock(TCPSocket)
      @connection.stub!(:socket).and_return(mock_socket)
      mock_socket.should_receive(:write).with(Connection::PROTOCOL).and_return(true)
      @connection.send_protocol_version.should eql(true)
    end

    it "should receive the protocol handshake response" do
      @connection.should respond_to(:protocol_handshake_ok?)
      mock_socket = mock(TCPSocket)
      @connection.stub!(:socket).and_return(mock_socket)
      mock_socket.should_receive(:recv).with(2).and_return(Connection::STATUS_OK)
      @connection.protocol_handshake_ok?.should eql(true)
    end

    it "should have a socket" do
      @connection.should respond_to(:socket)
    end
  end
end
