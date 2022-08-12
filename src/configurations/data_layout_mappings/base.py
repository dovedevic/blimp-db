import json

from pydantic import BaseModel, Field
from typing import Tuple

from src.configurations.hardware import HardwareConfiguration
from src.configurations.database import DatabaseConfiguration
from hardware import Bank
from generators import DatabaseRecordGenerator


RowMapping = Tuple[int, int]


class RowMappingSet(BaseModel):
    """Generic set of mapped row positions to their high-level representations"""
    data: RowMapping = Field(
        description="A row map representing the start row address for data and the number of rows this region contains")


class LayoutMetadata(BaseModel):
    """Metadata for standard DRAM data layout"""
    total_rows_for_records: int = Field(
        description="The total number of rows available to place data")
    total_records_processable: int = Field(
        description="The total number of records that this configuration can handle")


class DataLayoutConfiguration:
    """
    Defines the row/data layout configuration for a standard DRAM database bank. This base configuration places records
    side by side within the bank fitting as many as possible row by row

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [     record     ][     record     ][     reco-
    - rd     ]                                      -
    -                      ...                      -
    -                                               -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -                                               -
    -  ... [     record     ][     record     ]     -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """
    def __init__(self, hardware: HardwareConfiguration, database: DatabaseConfiguration):
        self._hardware_configuration = hardware
        self._database_configuration = database

        self._row_mapping_set = RowMappingSet(
            data=(0, hardware.bank_rows)
        )

        self._layout_metadata = LayoutMetadata(
            total_rows_for_records=hardware.bank_rows,
            total_records_processable=hardware.bank_size_bytes // database.total_record_size_bytes
        )

    def perform_data_layout(self, bank: Bank, record_generator: DatabaseRecordGenerator):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        temporary_row = 0
        bytes_in_temporary = 0
        generating_at_row = 0

        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        for record in record_generator.get_raw_records():
            if generating_at_row >= self._hardware_configuration.bank_rows:
                break  # done placing

            temporary_row <<= record_generator.record_size_bytes * 8
            temporary_row += record
            bytes_in_temporary += record_generator.record_size_bytes

            if bytes_in_temporary == self.hardware_configuration.row_buffer_size_bytes:
                # Place and reset
                bank.set_raw_row(generating_at_row, temporary_row)
                temporary_row = 0
                bytes_in_temporary = 0
                generating_at_row += 1
            elif bytes_in_temporary > self._hardware_configuration.row_buffer_size_bytes:
                # Overshot, chunk
                bytes_overshot = bytes_in_temporary - self._hardware_configuration.row_buffer_size_bytes
                overshot_mask = (2**(bytes_overshot * 8)) - 1
                overshot_row = temporary_row & overshot_mask

                temporary_row >>= bytes_overshot * 8
                bank.set_raw_row(generating_at_row, temporary_row)

                temporary_row = overshot_row
                bytes_in_temporary = bytes_overshot
                generating_at_row += 1
            else:
                # Undershot, continue loading data
                pass

        # Either no more records to process or we ran out of rows
        if generating_at_row < self._hardware_configuration.bank_rows:
            # Place what we have left
            bytes_left_in_row = self.hardware_configuration.row_buffer_size_bytes - bytes_in_temporary
            temporary_row <<= bytes_left_in_row * 8
            bank.set_raw_row(generating_at_row, temporary_row)

    @property
    def row_mapping(self):
        """Return the row address mapping for this configuration"""
        return self._row_mapping_set

    @property
    def layout_metadata(self):
        """Return the data layout metadata about this configuration"""
        return self._layout_metadata

    @property
    def hardware_configuration(self):
        """Get the internal hardware configuration"""
        return self._hardware_configuration

    @property
    def database_configuration(self):
        """Get the internal user-defined database configuration"""
        return self._database_configuration

    def display(self):
        """Dump the configuration into the console for visual inspection"""
        print("Hardware Configuration:")
        self._hardware_configuration.display()

        print("Database Configuration:")
        self._database_configuration.display()

        print("Row Mapping:")
        print(json.dumps(self.row_mapping.dict(), indent=4))

        print("Layout Metadata:")
        print(json.dumps(self.layout_metadata.dict(), indent=4))

    def save(self, path: str, compact=False):
        """
        Save the layout configuration as a JSON object

        @param path: The path and filename to save the configuration
        @param compact: Whether the JSON saved should be compact or indented
        """
        configuration = {
            "hardware": self._hardware_configuration.dict(),
            "database": self._database_configuration.dict(),
            "meta": {
                "row_mapping_set": self._row_mapping_set.dict(),
                "layout_metadata": self._layout_metadata.dict()
            }
        }
        with open(path, "w") as fp:
            if compact:
                json.dump(configuration, fp)
            else:
                json.dump(configuration, fp, indent=4)

    @classmethod
    def load(cls, path: str,
             hardware_config: callable=HardwareConfiguration,
             database_config: callable=DatabaseConfiguration
             ):
        """
        Load a layout configuration object

        @param path: The path and filename to load the configuration
        @param hardware_config: The constructor for a HardwareConfiguration. Defaults to `HardwareConfiguration`
        @param database_config: The constructor for a DatabaseConfiguration. Defaults to `DatabaseConfiguration`
        """
        with open(path, 'r') as fp:
            configuration = json.load(fp)
            return cls(hardware_config(**configuration["hardware"]),
                       database_config(**configuration["database"]))
