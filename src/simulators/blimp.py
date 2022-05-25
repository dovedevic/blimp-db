from src.configurations.bank_layout import BlimpBankLayoutConfiguration
from src.hardware.bank import BlimpBank
from src.generators.records import DatabaseRecordGenerator
from src.simulators.simulator import SimulatedBank
from src.simulators.result import RuntimeResult
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
        self.configuration = layout_configuration
        self.bank_hardware = bank_hardware

        self.registers = {
            self._v0(): [],
            self._v1(): [],
            self._v2(): [],
            self._instr_buffer(): [],
            self._data_pad(): []
        }

        self._logger.info(f"simulator loaded")

    def layout(self, record_set: DatabaseRecordGenerator, **kwargs):
        """
        Given a record set, horizontally layout data record by record, optionally reset and initialize hitmaps

        @param record_set: The record generator to use for filling the bank
        @kwarg reset_hitmaps: Default (True); When performing layout, reset/initialize all hitmaps
        @kwarg hitmap_default_value: Default (True); If @reset_hitmaps is set, set hitmaps to this initial value
        """
        self._logger.info(f"beginning blimp layout procedure")
        performance.start_performance_tracking()

        # Place BLIMP Records horizontally
        self.place_blimp_records(record_set)

        # Reset/Initialize the hitmaps if specified
        reset_hitmaps = kwargs.get('reset_hitmaps', True)
        if reset_hitmaps:
            self._logger.info(f"beginning hitmap reset/initialization")
            # If we are resetting hitmaps, get the value to use when resetting
            hitmap_default_value = kwargs.get('hitmap_default_value', True)
            self.reset_all_hitmaps(hitmap_default_value)
        self._logger.info(f"blimp layout completed in {performance.end_performance_tracking()}s")

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

    @staticmethod
    def _v0():
        """Register name for the pseudo-register linked to the row buffer"""
        return "v0"

    @staticmethod
    def _v1():
        """Register name for the first BLIMP-V vector register"""
        return "v1"

    @staticmethod
    def _v2():
        """Register name for the second BLIMP-V vector register"""
        return "v2"

    @staticmethod
    def _instr_buffer():
        """Register name for the Instruction Buffer"""
        return "instr_buffer"

    @staticmethod
    def _data_pad():
        """Register name for the Data Pad"""
        return "data_pad"

    @property
    def blimp_v0(self):
        """BLIMP V0 pseudo-row-buffer register"""
        return self._v0()

    @property
    def blimp_v1(self):
        """BLIMP-V V1 vector register"""
        return self._v1()

    @property
    def blimp_v2(self):
        """BLIMP-V V2 vector register"""
        return self._v2()

    @property
    def blimp_instruction_buffer(self):
        """BLIMP Instruction Buffer register"""
        return self._instr_buffer()

    @property
    def blimp_data_scratchpad(self):
        """BLIMP Data Scratchpad register"""
        return self._data_pad()

    def blimp_cycle(self, cycles=1, label="", return_labels=True) -> RuntimeResult:
        """Perform a specified number of BLIMP cycles"""
        if cycles <= 0:
            raise ValueError("argument 'cycles' cannot be less than one")
        runtime = RuntimeResult(
            self.configuration.hardware_configuration.time_per_blimp_cycle_ns,
            label if return_labels else ""
        )
        for c in range(cycles - 1):
            runtime.step(self.configuration.hardware_configuration.time_per_blimp_cycle_ns)
        return runtime

    def blimp_begin(self, return_labels=True) -> RuntimeResult:
        """Set the BLIMP-enable signal high to begin BLIMP bank operation"""

        # Initialize the BLIMP instruction buffer register to the first row, others are garbage (but loaded here)
        for r in self.registers:
            self.blimp_load_register(r, 0)

        return RuntimeResult(
            self.configuration.hardware_configuration.time_to_row_activate_ns,
            "BLIMP ENABLE" if return_labels else ""
        ) + self.blimp_cycle(cycles=5, label="; setup", return_labels=return_labels)

    def blimp_end(self, return_labels=True) -> RuntimeResult:
        """Set the BLIMP-enable signal low to complete BLIMP bank operation"""
        # All we are simulating is a row access and read to the BLIMP transfer mux
        return RuntimeResult(
            self.configuration.hardware_configuration.time_to_row_activate_ns,
            "BLIMP DISABLE" if return_labels else ""
        )

    def blimp_load_register(self, register, row: int, return_labels=True) -> RuntimeResult:
        """Fetch data into a specified internal register, via v0"""
        # Sanity checking
        if register not in self.registers:
            raise RuntimeError(f"Register '{register}' does not exist")
        result = self.blimp_cycle(return_labels=return_labels)

        # Fetch the row via the row buffer
        self.registers[self.blimp_v0] = self.bank_hardware.get_row_bytes(row)
        result += RuntimeResult(
            self.configuration.hardware_configuration.time_to_row_activate_ns,
            f"mem[{row}] -> {self.blimp_v0}" if return_labels else ""
        )

        # The row buffer (and now v0) is loaded with data, transfer it via the mux if necessary
        if register != self.blimp_v0:
            self.registers[register] = self.registers[self.blimp_v0]

            # Add time to transfer the row buffer/v0 to the specified mux destination
            result += RuntimeResult(
                self.configuration.hardware_configuration.time_to_v0_transfer_ns,
                f"\t{self.blimp_v0} -> {register}" if return_labels else ""
            )

        # Return the result of the operation
        return result

    def blimp_save_register(self, register, row: int, return_labels=True) -> RuntimeResult:
        """Save a specified internal register into a row, via v0"""
        # Sanity checking
        if register not in self.registers:
            raise RuntimeError(f"Register '{register}' does not exist")
        result = self.blimp_cycle(return_labels=return_labels)

        # Transfer data to v0 only when we aren't already operating on it
        if register != self.blimp_v0:
            # Transfer the register via the mux to v0/row buffer
            self.registers[self.blimp_v0] = self.registers[register]
            result += RuntimeResult(
                self.configuration.hardware_configuration.time_to_v0_transfer_ns,
                f"\t{register} -> {self.blimp_v0}" if return_labels else ""
            )

        # Save v0 into the bank memory
        self.bank_hardware.set_row_bytes(row, self.registers[self.blimp_v0])
        result += RuntimeResult(
            self.configuration.hardware_configuration.time_to_row_activate_ns,
            f"{self.blimp_v0} -> mem[{row}]" if return_labels else ""
        )

        # Return the result of the operation
        return result
