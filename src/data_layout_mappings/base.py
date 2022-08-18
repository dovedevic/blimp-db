import json

from pydantic import BaseModel, Field
from typing import Tuple

from configurations.hardware import HardwareConfiguration
from configurations.database import DatabaseConfiguration
from hardware import Bank
from generators import DatabaseRecordGenerator
from data_layout_mappings.methods import perform_record_packed_horizontal_layout


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
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_record_packed_horizontal_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=record_generator,
            limit=self.layout_metadata.total_records_processable
        )

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
