require 'json'
require 'voldemort-rb'

class VoldemortJsonBinarySerializer
  attr_accessor :has_version
  attr_accessor :type_def_versions
  
  BYTE_MIN_VAL = -128
  SHORT_MIN_VAL = -32768
  SHORT_MAX_VAL = 2 ** 15 - 1
  INT_MIN_VAL = -2147483648
  LONG_MIN_VAL = -9223372036854775808
  FLOAT_MIN_VAL = 2 ** -149
  DOUBLE_MIN_VAL = 2 ** -1074
  
  def initialize(type_def_versions)
    @has_version = true
    @type_def_versions = {}
    
    # convert versioned json strings to ruby objects
    type_def_versions.each_pair do |version, json_type_def_version|
      @type_def_versions[version.to_i] = get_type_def(json_type_def_version)
    end
  end
  
  def to_signed(unsigned, bits)
    max_unsigned = 2 ** bits
    max_signed = 2 ** (bits - 1)
    to_signed = proc { |n| (n >= max_signed) ? n - max_unsigned : n }
    return to_signed[unsigned]
  end
  
  def get_type_def(json_type_def_version)
    # replace all single quotes with " since the JSON parser wants it this way
    json_type_def_version = json_type_def_version.gsub(/\'/, '"')
    
    if((json_type_def_version =~ /[\{\[]/) == 0)
      # check if the json is a list or string, since these are 
      # the only ones that JSON.parse() will work with
      return JSON.parse(json_type_def_version)
    else
      # otherwise it's a primitive, so just strip the quotes
      return json_type_def_version.gsub(/\"/, '')
    end
  end
  
  def read_slice(length, bytes)
    substr = bytes[0, length]
    bytes.slice!(0..length - 1)
    return substr
  end
  
  # handle serialization
  
  def to_bytes(object)
    bytes = ''
    newest_version = 0 # TODO get highest number from map
    type_def = @type_def_versions[newest_version]
    
    if(@has_version)
      bytes << newest_version.chr
    end
    
    bytes << write(object, type_def)
    
    return bytes
  end

  def write(object, type)
    bytes = ''
    
    if(type.kind_of? Hash)
    	if(object != nil && !object.kind_of?(Hash))
    	  # TODO throw exception
    	else
    	  bytes << write_map(object, type)
    	end
    elsif(type.kind_of? Array)
    	if(object != nil && !object.kind_of?(Array))
    	  # TODO throw exception
    	else
    	  bytes << write_list(object, type)
    	end
    else
      case(type)
        when 'string'
          bytes << write_string(object)
        when 'int8'
          bytes << write_int8(object)
        when 'int16'
          bytes << write_int16(object)
        when 'int32'
          bytes << write_int32(object)
        when 'int64'
          bytes << write_int64(object)
        when 'float32'
          bytes << write_float32(object)
        when 'float64'
          bytes << write_float64(object)
        when 'date'
          bytes << write_date(object)
        when 'bytes'
          bytes << write_bytes(object)
        when 'boolean'
          bytes << write_boolean(object)
        else
          # TODO throw unsupported type exception
      end
    end
    
    if(bytes == '')
      return nil
    end
    
    return bytes
  end

  def write_boolean(object)
    bytes = ''
    
    if(object == nil)
      bytes << [BYTE_MIN_VAL].pack('c')
    elsif(object)
      bytes << [0x1].pack('c')
    else
      bytes << [0x0].pack('c')
    end
    
    return bytes
  end
  
  def write_string(object)
    return write_bytes(object)
  end
  
  def write_int8(object)
    bytes = ''
    
    if(object == BYTE_MIN_VAL)
      # TODO throw underflow exception
    else
      if(object == nil)
        object = BYTE_MIN_VAL
      end
      
      bytes << [object].pack('c')
    end
    
    return bytes
  end
  
  def write_int16(object)
    bytes = ''
    
    if(object == SHORT_MIN_VAL)
      # TODO throw underflow exception
    else
      if(object == nil)
        object = SHORT_MIN_VAL
      end
      
      bytes << [object].pack('n')
    end
    
    return bytes
  end
  
  def write_int32(object)
    bytes = ''
    
    if(object == INT_MIN_VAL)
      # TODO throw underflow exception
    else
      if(object == nil)
        object = INT_MIN_VAL
      end
      
      # reverse here to switch little endian to big endian
      # this is because pack('N') is choking on 'bigint', wtf?
      bytes << [object].pack('i').reverse
    end
    
    return bytes
  end
  
  def write_int64(object)
    bytes = ''
    
    if(object == LONG_MIN_VAL)
      # TODO throw underflow exception
    else
      if(object == nil)
        object = LONG_MIN_VAL
      end
      
      # reverse here to switch little endian to big endian
      # this is because pack('N') is choking on 'bigint', wtf?
      bytes << [object].pack('q').reverse
    end
    
    return bytes
  end
  
  def write_float32(object)
    bytes = ''
    
    if(object == FLOAT_MIN_VAL)
      # TODO throw underflow exception
    else
      if(object == nil)
        object = FLOAT_MIN_VAL
      end
      
      bytes << [object].pack('g')
    end
    
    return bytes
  end
  
  def write_float64(object)
    bytes = ''
    
    if(object == DOUBLE_MIN_VAL)
      # TODO throw underflow exception
    else
      if(object == nil)
        object = DOUBLE_MIN_VAL
      end
      
      bytes << [object].pack('G')
    end
    
    return bytes
  end
  
  def write_date(object)
    bytes = ''
    
    if(object == LONG_MIN_VAL)
      # TODO throw underflow exception
    else
      if(object == nil)
        bytes << write_int64(nil)
      else
        bytes << write_int64((object.to_f * 1000).to_i)
      end
    end
    
    return bytes
  end
  
  def write_bytes(object)
    bytes = ''
    
    if(object == nil)
      bytes << write_int16(-1)
    elsif(object.length < SHORT_MAX_VAL)
      bytes << write_int16(object.length)
      bytes << object
    else
      # TODO throw "length too long to serialize" exception
    end
    
    return bytes
  end
  
  def write_map(object, type)
    bytes = ''
    
    if(object == nil)
      bytes << [-1].pack('c')
    else
      bytes << [1].pack('c')
      
      if(object.length != type.length)
        # TODO throw exception here.. invalid map serialization, expected: but got 
      else
        type.sort.each do |type_pair|
          key = type_pair.first
          subtype = type_pair.last
          
          if(!object.has_key? key)
            # TODO throw "missing property exception"
          else
            bytes << write(object[key], subtype)
          end
        end
      end
    end
    
    return bytes
  end
  
  def write_list(object, type)
    bytes = ''
    
    if(type.length != 1)
      # TODO throw new exception (expected single type in list)
    else
      entry_type = type.first
      
      if(object == nil)
        bytes << write_int16(-1)
      elsif(object.length < SHORT_MAX_VAL)
        bytes << write_int16(object.length)
        object.each do |o|
          bytes << write(o, entry_type)
        end
      else
        # TODO throw serialization exception
      end
    end
    
    return bytes
  end
  
  # handle deserialization
  
  def to_object(bytes)
    version = 0
    
    if(@has_version)
      version = read_slice(1, bytes).to_i
    end
    
    type = @type_def_versions[version]
    
    if(type == nil)
      # TODO throw exception here
    end
    
    return read(bytes, type)
  end
  
  def read(bytes, type)
    if(type.kind_of? Hash)
      return read_map(bytes, type)
    elsif(type.kind_of? Array)
      return read_list(bytes, type)
    else
      case(type)
        when 'string'
          return read_bytes(bytes)
        when 'int8'
          return read_int8(bytes)
        when 'int16'
          return read_int16(bytes)
        when 'int32'
          return read_int32(bytes)
        when 'int64'
          return read_int64(bytes)
        when 'float32'
          return read_float32(bytes)
        when 'float64'
          return read_float64(bytes)
        when 'date'
          return read_date(bytes)
        when 'bytes'
          return read_bytes(bytes)
        when 'boolean'
          return read_boolean(bytes)
        # TODO default throw unknown type exception
      end
    end
  end
  
  def read_map(bytes, type)
    # convert to char to string, and string to int
    if(read_slice(1, bytes).unpack('c').to_s.to_i == -1)
      return nil
    else
      object = {}
      
      type.sort.each do |type_pair|
        name = type_pair.first
        sub_type = type_pair.last
        object[name] = read(bytes, sub_type)
      end
      
      return object
    end
  end
  
  def read_list(bytes, type)
    size = read_int16(bytes)
    if(size < 0)
      return nil
    else
      object = []
      
      size.times { object << read(bytes, type.first) }
      
      return object
    end
  end
  
  def read_boolean(bytes)
    b = read_slice(1, bytes).unpack('c').first
    
    if(b < 0)
      return nil
    elsif(b == 0)
      return false
    else
      return true
    end
  end
  
  def read_int8(bytes)
    b = read_slice(1, bytes).unpack("c").first.to_i
    
    if(b == BYTE_MIN_VAL)
      return nil
    end
    
    return b
  end
  
  def read_int16(bytes)
    s = to_signed(read_slice(2, bytes).unpack("n").first, 16)

    if(s == SHORT_MIN_VAL)
      return nil
    end
    
    return s
  end
  
  def read_int32(bytes)
    # reverse here to switch little endian to big endian
    # this is because pack('N') is choking on 'bigint', wtf?
    i = read_slice(4, bytes).reverse.unpack("i").first.to_i
    
    if(i == INT_MIN_VAL)
      return nil
    end
    
    return i
  end
  
  def read_int64(bytes)
    # reverse here to switch little endian to big endian
    # this is because pack('N') is choking on 'bigint', wtf?
    l = read_slice(8, bytes).reverse.unpack("q").first.to_i
    
    if(l == LONG_MIN_VAL)
      return nil
    end
    
    return l
  end
  
  def read_float32(bytes)
    f = read_slice(4, bytes).unpack("g").first.to_f
    
    if(f == FLOAT_MIN_VAL)
      return nil
    end
    
    return f
  end
  
  def read_float64(bytes)
    d = read_slice(8, bytes).unpack("G").first.to_f
    
    if(d == DOUBLE_MIN_VAL)
      return nil
    end
    
    return d
  end
  
  def read_date(bytes)
    d = read_int64(bytes)
    
    if(d != nil)
      d = Time.at((d / 1000).to_i, d % 1000)
    end
    
    return d
  end
  
  def read_bytes(bytes)
    size = read_int16(bytes)
    
    if(size < 0)
      return nil
    else
      return read_slice(size, bytes)
    end
  end
end

class VoldemortPassThroughSerializer
	def initialize(map)
	end
	
	def to_bytes(bytes)
	  bytes
	end
	
	def to_object(object)
	  object
	end
end