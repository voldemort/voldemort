class VoldemortException < StandardError
  def initialize(message, code = 1)
    @code = code
    @message = message
  end
end