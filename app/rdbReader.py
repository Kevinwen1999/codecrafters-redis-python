import struct
from datetime import datetime, timedelta, MAXYEAR
infinite_time = datetime(MAXYEAR - 1, 12, 31, 23, 59, 59, 999999)

class RDBParser:
    def __init__(self, filename):
        self.filename = filename
        self.metadata = {}
        self.databases = []
        self.end_checksum = None

    def parse(self):
        try:
            with open(self.filename, 'rb') as f:
                self._parse_header(f)
                while True:
                    byte = f.read(1)
                    if not byte:
                        break
                    byte = ord(byte)
                    
                    # Parse metadata sections (FA marks metadata)
                    if byte == 0xFA:
                        self._parse_metadata(f)
                    # Parse database sections (FE marks a database subsection)
                    elif byte == 0xFE:
                        self.databases.append(self._parse_database(f))
                    # Parse the end of file section (FF marks EOF)
                    elif byte == 0xFF:
                        self.end_checksum = f.read(8)
                        break
                    else:
                        raise ValueError(f"Unknown section start: {hex(byte)}")
        except FileNotFoundError:
            print(f"Error: The file '{self.filename}' does not exist.")
        except Exception as e:
            print(f"An error occurred while parsing the file: {e}")

    def _parse_header(self, f):
        header = f.read(9)
        if header != b'REDIS0011':
            raise ValueError("Invalid RDB file format or version")
        print(f"Header: {header.decode()}")

    def _parse_metadata(self, f):
        name = self._read_string(f)
        value = self._read_string(f)
        self.metadata[name] = value
        print(f"Metadata - {name}: {value}")

    def _parse_database(self, f):
        db_index = self._read_size(f)
        print(f"Database index: {db_index}")
        database = {'index': db_index, 'hash_table': []}
        
        # Parse hash table size
        f.read(1)  # Skip FB
        key_size = self._read_size(f)
        expire_size = self._read_size(f)
        print(f"Key-Value Table Size: {key_size}, Expire Table Size: {expire_size}")

        # Parse key-value pairs
        for _ in range(key_size):
            entry = self._parse_key_value(f)
            database['hash_table'].append(entry)
        
        print(database)
        return database

    def _parse_key_value(self, f):
        entry = {}
        entry['expire'] = infinite_time

        expire_type = ord(f.peek(1)[:1])  # Peek at the next byte without consuming it

        # Optional expire information
        if expire_type == 0xFD:
            f.read(1)  # Consume the expiration marker byte
            entry['expire'] = datetime.fromtimestamp(struct.unpack('<I', f.read(4))[0])  # Expire timestamp in seconds
        elif expire_type == 0xFC:
            f.read(1)  # Consume the expiration marker byte
            entry['expire'] = datetime.fromtimestamp(struct.unpack('<Q', f.read(8))[0] / 1000) # Expire timestamp in milliseconds



        # Value type
        entry['type'] = ord(f.read(1))

        # Key and value
        entry['key'] = self._read_string(f)
        entry['value'] = self._read_string(f)
        print(f"Entry - Key: {entry['key']}, Value: {entry['value']}, Expire: {entry.get('expire', 'None')}")

        return entry

    def _read_size(self, f):
        size_byte = ord(f.read(1))
        if size_byte & 0xC0 == 0xC0:
            # Special encoding for integers
            encoding_type = size_byte
            if encoding_type == 0xC0:  # 8-bit integer
                value = ord(f.read(1))
            elif encoding_type == 0xC1:  # 16-bit integer
                value = struct.unpack('<H', f.read(2))[0]
            elif encoding_type == 0xC2:  # 32-bit integer
                value = struct.unpack('<I', f.read(4))[0]
            else:
                raise ValueError("Unsupported integer encoding")
            return ("int", value)
        elif size_byte & 0x80:
            size = ((size_byte & 0x3F) << 8) | ord(f.read(1))
        elif size_byte == 0xFE:
            size = struct.unpack('>I', f.read(4))[0]
        else:
            size = size_byte
        return size

    def _read_string(self, f):
        length = self._read_size(f)
        if length is None:
            return None
        elif isinstance(length, tuple) and length[0] == "int":
            # Handle special integer encoding
            return str(length[1])
        else:
            return f.read(length).decode()

    def get_metadata(self):
        return self.metadata

    def get_databases(self):
        return self.databases

    def get_checksum(self):
        return self.end_checksum


# Usage example
if __name__ == "__main__":
    parser = RDBParser("dump.rdb")
    parser.parse()
    print("Metadata:", parser.get_metadata())
    print("Databases:", parser.get_databases())
    print("End Checksum:", parser.get_checksum())