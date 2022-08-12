from hardware import Bank
from configurations.hardware.ambit import AmbitHardwareConfiguration
from utils.bitmanip import int_to_byte_array


class AmbitBank(Bank):
    """Defines operations for an AMBIT DRAM Bank"""

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
