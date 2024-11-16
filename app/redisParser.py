class RedisParser:
    def parse(self, data):
        if not data:
            raise ValueError("No data to parse")

        # Check the first character to determine the type
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
            raise ValueError(f"Unknown RESP type: {data}")

    def parse_simple_string(self, data):
        return data[1:-2].decode('utf-8')

    def parse_error(self, data):
        return Exception(data[1:-2].decode('utf-8'))

    def parse_integer(self, data):
        return int(data[1:-2])

    def parse_bulk_string(self, data):
        # Get length of the bulk string
        end_of_len = data.find(b'\r\n')
        length = int(data[1:end_of_len])
        if length == -1:
            return None  # RESP `nil` bulk string
        
        start = end_of_len + 2
        end = start + length
        return data[start:end].decode('utf-8')

    def parse_array(self, data):
        # Get length of the array
        end_of_len = data.find(b'\r\n')
        length = int(data[1:end_of_len])
        if length == -1:
            return None  # RESP `nil` array

        elements = []
        start = end_of_len + 2
        for _ in range(length):
            element, next_start = self.parse_element(data[start:])
            elements.append(element)
            start += next_start
        return elements

    def parse_element(self, data):
        # Parses an individual element in an array
        if data.startswith(b'+'):
            element = self.parse_simple_string(data)
            return element, len(element) + 3  # +3 accounts for '+', data length, '\r\n'
        elif data.startswith(b'-'):
            element = self.parse_error(data)
            return element, len(element) + 3
        elif data.startswith(b':'):
            element = self.parse_integer(data)
            return element, len(str(element)) + 3
        elif data.startswith(b'$'):
            end_of_len = data.find(b'\r\n')
            length = int(data[1:end_of_len])
            total_length = end_of_len + 2 + length + 2  # includes the initial length, string, and `\r\n`
            element = self.parse_bulk_string(data[:total_length])
            return element, total_length
        elif data.startswith(b'*'):
            array = self.parse_array(data)
            return array, len(data)
        else:
            raise ValueError("Unknown RESP type in array element")
        



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
        hex_string = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        byte_string = bytes.fromhex(hex_string)
        length_string = f"${len(byte_string)}\r\n"
        return length_string.encode() + byte_string


    def to_resp_error(self, data):
        # Represents an error message
        return f"-{str(data)}\r\n"