import json
import logging

from src.configurations import HardwareConfiguration
from utils import performance
from utils.bitmanip import byte_array_to_int, int_to_byte_array


class Bank:
    """Defines bank operations for a generic DRAM Bank"""
    def __init__(self, configuration: HardwareConfiguration, memory: list=None, default_byte_value: int=0xff):
        self._config = configuration
        self._logger = logging.getLogger(self.__class__.__name__)
        self.default_byte_value = default_byte_value

        # Ensure the default value is only one byte
        if default_byte_value < 0 or default_byte_value >= 256:
            raise ValueError("default byte value must be expressable with a single byte")

        # If a bank file was provided, ensure it is valid with the configuration
        if memory:
            # Too few bank rows?
            if len(memory) != configuration.bank_rows:
                raise ValueError("the bank size does not match the configuration")

            # All rows represent at least the maximum supported by the row buffer?
            if any(row >= 2**(configuration.row_buffer_size_bytes * 8) or row < 0 for row in memory):
                raise ValueError("the row buffer size does not match the configuration")

        if memory:
            self.memory = memory
        else:
            self.memory = []
            for row in range(configuration.bank_rows):
                value = 0
                for _ in range(configuration.row_buffer_size_bytes):
                    value <<= 8
                    value += default_byte_value
                self.memory.append(value)
        self._logger.info(f"bank loaded with {'initial' if memory else 'null'} memory state")

    @property
    def hardware_configuration(self):
        """Get this bank hardware's configuration"""
        return self._config

    def get_raw_row(self, row_index: int):
        """Fetch a row by its index and return integer representation of the byte array"""
        self._logger.debug(f"bank fetch at {hex(row_index * self._config.row_buffer_size_bytes)} (row {row_index})")
        return self.memory[row_index]

    def set_raw_row(self, row_index: int, value: int):
        """Set a specified row with a provided integer value acting as a raw byte array"""
        self._logger.debug(f"bank store at {hex(row_index * self._config.row_buffer_size_bytes)} (row {row_index})")

        # Ensure this is row-buffer sized
        if value >= 2**(self._config.row_buffer_size_bytes*8) or value < 0:
            raise ValueError("raw value bit width dimension does not match row buffer size")

        # Passed checks, set and return row
        self.memory[row_index] = value
        return value

    def get_row_bytes(self, row_index: int):
        """Fetch a row by its index and return the byte array"""
        raw_value = self.get_raw_row(row_index)
        byte_array = int_to_byte_array(raw_value, self._config.row_buffer_size_bytes)
        return byte_array

    def set_row_bytes(self, row_index: int, byte_array: list):
        """Set a specified row with a provided byte array"""

        # Ensure this is row-buffer sized
        if len(byte_array) != self._config.row_buffer_size_bytes:
            raise ValueError("byte array dimension does not match row buffer size")

        raw_result = byte_array_to_int(byte_array)

        # Save the raw value
        self.set_raw_row(row_index, raw_result)
        return raw_result

    def save(self, path: str, dump_with_ascii=True):
        """Save the current state of the bank's memory with the system configuration and a hexdump"""
        self._logger.info(f"saving memory state into {path}")
        performance.start_performance_tracking()
        with open(path, 'w') as fp:
            # Write the system configuration
            self._logger.info(f"saving memory system configuration")
            json.dump(self._config.dict(), fp)
            fp.write("\n")

            # Hexdump the memory with address, hexdump, ascii dump
            self._logger.info(f"saving memory dump")
            for idx in range(len(self.memory)):
                address_line = f'%08X:  ' % (idx * self._config.row_buffer_size_bytes)
                byte_string = ""
                ascii_string = ""
                row = self.get_row_bytes(idx)
                for byte in row:
                    byte_string += "%02X " % byte
                    if dump_with_ascii:
                        if 0x20 <= byte <= 0x7E:
                            ascii_string += chr(byte)
                        else:
                            ascii_string += '.'
                fp.write(address_line)
                fp.write(byte_string)
                fp.write(' ')
                fp.write(ascii_string)
                fp.write('\n')
        self._logger.info(f"memory state saved in {performance.end_performance_tracking()}s")

    @classmethod
    def load(cls, path: str):
        """Load a saved bank memory dump"""
        _logger = logging.getLogger(cls.__name__)
        _logger.info(f"loading memory state from {path}")
        performance.start_performance_tracking()
        with open(path, 'r') as fp:
            # Load the system configuration
            preamble = fp.readline()
            _logger.info(f"interpreting memory system configuration")
            configuration = HardwareConfiguration(**json.loads(preamble))

            _logger.info(f"interpreting memory dump")
            # Parse the hexdump
            memory_array = []
            try:
                for line in fp.readlines():
                    # Extract out the areas
                    address, str_byte_array, ascii_array = line.strip().split("  ", 2)
                    # Convert the byte array hext string to integers
                    hex_byte_array = str_byte_array.split(" ")
                    byte_array = [int(byte, 16) for byte in hex_byte_array]
                    raw_result = byte_array_to_int(byte_array)
                    # Add this row to our memory array
                    memory_array.append(raw_result)
            except ValueError:
                raise ValueError("File contains non-parseable memory bytes")
        _logger.info(f"memory state loaded in {performance.end_performance_tracking()}s")
        return cls(configuration, memory=memory_array)
