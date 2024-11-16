class RedisParser:
    HARD_CODED_RDB_HEX = (
        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473"
        "c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d626173"
        "65c000fff06e3bfec0ff5aa2"
    )

    def parse(self, data):
        if not data:
            raise ValueError("No data to parse")
        
        results = []
        offset = 0

        while offset < len(data):
            element, next_offset = self.parse_element(data[offset:])
            results.append(element)
            offset += next_offset

        return results

    def parse_simple_string(self, data):
        end_of_str = data.find(b'\r\n')
        if end_of_str == -1:
            raise ValueError("Malformed RESP simple string")
        return data[1:end_of_str].decode('utf-8'), end_of_str + 2

    def parse_error(self, data):
        end_of_err = data.find(b'\r\n')
        if end_of_err == -1:
            raise ValueError("Malformed RESP error")
        return Exception(data[1:end_of_err].decode('utf-8')), end_of_err + 2

    def parse_integer(self, data):
        end_of_int = data.find(b'\r\n')
        if end_of_int == -1:
            raise ValueError("Malformed RESP integer")
        return int(data[1:end_of_int]), end_of_int + 2

    """ def parse_bulk_string(self, data):
        end_of_len = data.find(b'\r\n')
        if end_of_len == -1:
            raise ValueError("Malformed RESP bulk string length")
        length = int(data[1:end_of_len])
        if length == -1:
            return None, end_of_len + 2  # RESP `nil` bulk string
        
        start = end_of_len + 2
        end = start + length
        if len(data) < end + 2:
            raise ValueError("Malformed RESP bulk string content")
        return data[start:end].decode('utf-8'), end + 2 """
    
    def parse_bulk_string(self, data):
        # Find the end of the bulk string length declaration
        end_of_len = data.find(b'\r\n')
        if end_of_len == -1:
            raise ValueError("Malformed RESP bulk string length")

        # Get the length of the bulk string contents
        length = int(data[1:end_of_len])

        # Calculate start and end of the bulk string contents
        start = end_of_len + 2
        end = start + length

        # Check if the content matches the hardcoded RDB file hex value
        content_hex = data[start:end].hex()
        if content_hex == self.HARD_CODED_RDB_HEX:
            return "RDB File Content", end

        # Otherwise, handle as a normal bulk string
        if len(data) < end + 2 or data[end:end + 2] != b'\r\n':
            raise ValueError("Malformed RESP bulk string content")

        return data[start:end].decode('utf-8'), end + 2

    def parse_array(self, data):
        end_of_len = data.find(b'\r\n')
        if end_of_len == -1:
            raise ValueError("Malformed RESP array length")
        length = int(data[1:end_of_len])
        if length == -1:
            return None, end_of_len + 2  # RESP `nil` array

        elements = []
        offset = end_of_len + 2
        for _ in range(length):
            element, next_offset = self.parse_element(data[offset:])
            elements.append(element)
            offset += next_offset
        return elements, offset

    def parse_element(self, data):
        if data.startswith(b'+'):
            return self.parse_simple_string(data)
        elif data.startswith(b'-'):
            return self.parse_error(data)
        elif data.startswith(b':'):
            return self.parse_integer(data)
        elif data.startswith(b'$'):
            return self.parse_bulk_string(data)
        elif data.startswith(b'*'):
            return self.parse_array(data)
        else:
            raise ValueError("Unknown RESP type")
        



    def to_resp(self, data):
        """Converts Python data types to RESP format."""
        if isinstance(data, str):
            return self.to_resp_string(data)
        elif isinstance(data, int):
            return self.to_resp_integer(data)
        elif isinstance(data, list) or isinstance(data, tuple):
            return self.to_resp_array(data)
        elif data is None:
            return self.to_resp_null()
        elif isinstance(data, Exception):
            return self.to_resp_error(data)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

    def to_resp_string(self, data):
        """ if '\r' in data or '\n' in data:
            # Convert to Bulk String if it contains line breaks
            return f"${len(data)}\r\n{data}\r\n"
        else:
            # Simple String otherwise
            return f"+{data}\r\n" """
        return f"${len(data)}\r\n{data}\r\n"
    
    def to_resp_simple_string(self, data):
        return f"+{data}\r\n"

    def to_resp_integer(self, data):
        return f":{data}\r\n"

    def to_resp_array(self, data):
        elements = ''.join([self.to_resp(element) for element in data])
        return f"*{len(data)}\r\n{elements}"

    def to_resp_null(self):
        # Represents a null bulk string
        return "$-1\r\n"
    
    def to_empty_RDB(self):
        hex_string = self.HARD_CODED_RDB_HEX
        byte_string = bytes.fromhex(hex_string)
        length_string = f"${len(byte_string)}\r\n"
        return length_string.encode() + byte_string


    def to_resp_error(self, data):
        # Represents an error message
        return f"-{str(data)}\r\n"