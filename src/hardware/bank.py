import json
import logging

from src.configurations.hardware import HardwareConfiguration, BlimpHardwareConfiguration, AmbitHardwareConfiguration
from utils import performance
from utils.bitmanip import byte_array_to_int, int_to_byte_array


class Bank:
    """Defines operations for a generic DRAM Bank"""
    def __init__(self, configuration: HardwareConfiguration, memory: list=None, default_byte_value: int=0xff):
        self._config = configuration
        self._logger = logging.getLogger(self.__class__.__name__)

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


class BlimpBank(Bank):
    """Defines operations for a BLIMP/-V DRAM Bank"""
    def __init__(self, configuration: BlimpHardwareConfiguration, memory: list=None, default_byte_value: int=0xff):
        super(BlimpBank, self).__init__(configuration, memory, default_byte_value)


class AmbitBank(BlimpBank):
    """Defines operations for a BLIMP/-V controlled AMBIT DRAM Bank"""

    def __init__(self, configuration: AmbitHardwareConfiguration, memory: list = None, default_byte_value: int = 0xff):
        super(AmbitBank, self).__init__(configuration, memory, default_byte_value)

    def get_inverted_row_bytes(self, row_index: int):
        """Fetch a row by its index and return the inverted byte array"""
        raw_inverted = self.get_inverted_raw_row(row_index)
        inverted_byte_array = int_to_byte_array(raw_inverted, self._config.row_buffer_size_bytes)
        return inverted_byte_array

    def get_inverted_raw_row(self, row_index: int):
        """Fetch a row by its index and return the inverted integer representation of the byte array"""
        self._logger.debug(f"inverting values at {hex(row_index * self._config.row_buffer_size_bytes)}")
        result = self.get_raw_row(row_index)
        inverted_result = (~result & (2**(self._config.row_buffer_size_bytes * 8) - 1))
        return inverted_result

    def copy_row(self, from_index: int, to_index: int):
        """Copy a row from a specified index and set it to another row index"""
        self._logger.debug(f"row copy from "
                      f"{hex(from_index * self._config.row_buffer_size_bytes)} to "
                      f"{hex(to_index * self._config.row_buffer_size_bytes)}")
        result = self.get_raw_row(from_index)
        result = self.set_raw_row(to_index, result)
        return result

    def tra_rows(self, row_index_a: int, row_index_b: int, row_index_c: int, invert=False):
        """
        Perform a Triple-Row-Activation (TRA) operation on three provided rows. Overwrites the values of all rows
        with the value of the TRA operation.
        """
        self._logger.debug(f"performing tra operation at addresses "
                      f"{hex(row_index_a * self._config.row_buffer_size_bytes)}, "
                      f"{hex(row_index_b * self._config.row_buffer_size_bytes)}, "
                      f"{hex(row_index_c * self._config.row_buffer_size_bytes)}")
        a = self.get_raw_row(row_index_a)
        b = self.get_raw_row(row_index_b)
        c = self.get_raw_row(row_index_c)

        # Ambit: In-memory accelerator for bulk bitwise operations using commodity DRAM technology
        # https://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=8686556&casa_token=lv7AHcUyJhIAAAAA:tcUppn7BlvPM1dlT3L1nOPfb3hCBF1AWUA3H_GdDgdLrZDLr1TXjOJf-724ai6aeMsKZspvx&tag=1
        # https://scholar.google.com/scholar?hl=en&as_sdt=0%2C39&q=ambit&btnG=
        # Section 3.1, Paragraph 3
        tra_value = a & b | b & c | c & a

        if invert:
            tra_value = ~tra_value & ((2**(self._config.row_buffer_size_bytes*8)) - 1)

        self.set_raw_row(row_index_a, tra_value)
        self.set_raw_row(row_index_b, tra_value)
        self.set_raw_row(row_index_c, tra_value)

        return tra_value
