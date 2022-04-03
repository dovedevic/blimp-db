from src.configurations.bank_layout import BlimpBankLayoutConfiguration
from src.hardware.bank import BlimpBank
from src.generators.records import DatabaseRecordGenerator
from src.simulators.simulator import SimulatedBank
from src.utils import performance


class SimulatedBlimpBank(SimulatedBank):
    """Defines simulation parameters for a BLIMP-capable DRAM Bank"""
    def __init__(
            self,
            layout_configuration: BlimpBankLayoutConfiguration,
            bank_hardware: BlimpBank,
            logger=None
            ):
        super(SimulatedBlimpBank, self).__init__(layout_configuration, bank_hardware, logger)
        self._logger.info(f"simulator loaded")

    def layout(self, record_set: DatabaseRecordGenerator, **kwargs):
        """
        Given a record set, horizontally layout data record by record, optionally reset and initialize hitmaps

        @param record_set: The record generator to use for filling the bank
        @kwarg reset_hitmaps: Default (True); When performing layout, reset/initialize all hitmaps
        @kwarg hitmap_default_value: Default (True); If @reset_hitmaps is set, set hitmaps to this initial value
        """
        self._logger.info(f"simulator beginning layout procedure")
        performance.start_performance_tracking()
        # Place BLIMP Records horizontally
        self.place_blimp_records(record_set)

        # Reset/Initialize the hitmaps if specified
        reset_hitmaps = kwargs.get('reset_hitmaps', True)
        if reset_hitmaps:
            self._logger.info(f"simulator beginning hitmap reset/initialization")
            # If we are resetting hitmaps, get the value to use when resetting
            hitmap_default_value = kwargs.get('hitmap_default_value', True)
            self.reset_all_hitmaps(hitmap_default_value)
        self._logger.info(f"simulator layout completed in {performance.end_performance_tracking()}s")

    def place_blimp_records(self, record_generator: DatabaseRecordGenerator):
        """Given a record generator and a database configuration, place records into the BLIMP-region horizontally"""
        self._logger.info(f"blimp horizontal layout beginning")
        performance.start_performance_tracking()
        base_record_row, record_row_count = self.configuration.address_mapping["records"]

        # See if we are in a multi-record-per-row configuration or multi-row-per-record
        records_per_row = self.configuration.hardware_configuration.row_buffer_size_bytes \
            // self.configuration.database_configuration.total_record_size_bytes
        records_placed = 0

        if records_per_row > 0:
            self._logger.info("performing multi-record row layout")
            # Multiple (or one) records per row; record size <= row buffer
            for row_index in range(record_row_count):
                # Construct a temporary location for the row / records
                records_in_row = []

                # Fetch all records in this row, if we are at the end, null pad with zeros
                for sub_record_index in range(records_per_row):
                    if records_placed < self.configuration.total_records_processable:
                        records_in_row.append(
                            record_generator.get_raw_record(row_index * records_per_row + sub_record_index)
                        )
                        records_placed += 1
                    else:
                        records_in_row.append(record_generator.get_null_record())

                # Construct the raw value byte array for the hardware
                raw_value = 0
                for raw in records_in_row:
                    raw_value <<= (record_generator.record_size_bytes * 8)
                    raw_value |= raw

                # Store this row with all the records placed
                self.bank_hardware.set_raw_row(
                    base_record_row + row_index,
                    raw_value
                )
        else:
            self._logger.info("performing multi-row record layout")
            # Multiple rows per record; row buffer < record size
            rows_per_record = self.configuration.database_configuration.total_record_size_bytes \
                // self.configuration.hardware_configuration.row_buffer_size_bytes

            # For all placeable records, extract row-buffer sized chunks and store them
            for record_index in range(self.configuration.total_records_processable):
                # Fetch/Generate the records
                record = record_generator.get_raw_record(record_index)

                # Chunk the record
                for sub_row_index in range(rows_per_record):
                    # Construct the mask for each chunk
                    row_buffer_mask = (2**(self.configuration.hardware_configuration.row_buffer_size_bytes * 8)) - 1
                    # Move the mask to the appropriate chunk
                    record_mask = row_buffer_mask << \
                        (rows_per_record - 1 - sub_row_index) * \
                        (self.configuration.hardware_configuration.row_buffer_size_bytes * 8)
                    # Mask the record to extract the chunk
                    masked_record = record & record_mask
                    # Realign the chunk
                    record_chunk = masked_record >> \
                        (rows_per_record - 1 - sub_row_index) * \
                        (self.configuration.hardware_configuration.row_buffer_size_bytes * 8)
                    # Save the chunk
                    self.bank_hardware.set_raw_row(
                        base_record_row + (record_index * rows_per_record) + sub_row_index,
                        record_chunk
                    )
        self._logger.info(f"data layout completed in {performance.end_performance_tracking()}s")

    def reset_hitmap(self, hitmap_index: int, value: bool=True):
        """
        Given a hitmap_index, reset the hitmap values to a provided value. Only resets hitmap bits up to the
        record count. For example, if a full hitmap bit mask is larger than the remaining records, these bits are
        ignored/nulled.
        """
        self._logger.info(f"hitmap[{hitmap_index}] bits are being reset to {'1' if value else '0'}")

        if hitmap_index >= self.configuration.database_configuration.hitmap_count:
            raise IndexError(f"No hitmap at index {hitmap_index} is present in this database configuration")

        # Get the base hitmap row, as well as the number of total hitmap rows
        base_hitmap_row, hitmap_row_count = self.configuration.address_mapping["hitmaps"]
        # Get the number of rows per hitmap
        rows_per_hitmap = hitmap_row_count // self.configuration.database_configuration.hitmap_count
        # Calculate this hitmaps row address
        hitmap_base_address = base_hitmap_row + rows_per_hitmap * hitmap_index

        # Reset all hitmap (subrows - 1) as they are guaranteed to be fully filled
        # There is guaranteed to be /at least one/ subrow since 0 subrows would not be a hitmap or would be OOB
        hitmap_bits = 0
        hitmap_sub_row = -1
        for hitmap_sub_row in range(rows_per_hitmap - 1):
            # Increment how many bits/records this subrow accounts for
            hitmap_bits += self.configuration.hardware_configuration.row_buffer_size_bytes * 8

            # Perform full row reset
            self.bank_hardware.set_raw_row(
                hitmap_base_address + hitmap_sub_row,
                ((2 ** (self.configuration.hardware_configuration.row_buffer_size_bytes * 8)) - 1) if value else 0
            )
        # Set the remaining row with only up to the remainder of bits/records processable
        remainder = self.configuration.total_records_processable - hitmap_bits
        null_remainder = self.configuration.hardware_configuration.row_buffer_size_bytes * 8 - remainder

        # Perform partial row reset
        self.bank_hardware.set_raw_row(
            hitmap_base_address + hitmap_sub_row + 1,
            ((2 ** remainder) - 1) << null_remainder if value else 0
        )

    def reset_all_hitmaps(self, value: bool=True):
        """Iterate over all configured hitmaps, reset them to a provided value"""
        self._logger.info("hitmaps beginning to be reset/initialized")
        performance.start_performance_tracking()
        for hitmap in range(self.configuration.database_configuration.hitmap_count):
            self.reset_hitmap(hitmap, value)
        self._logger.info(f"hitmaps reset/initialized in {performance.end_performance_tracking()}s")
