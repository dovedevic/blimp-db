from typing import Union

from src.queries.query import Query
from src.simulators.result import RuntimeResult, SimulationResult
from src.utils import bitmanip
from src.data_layout_mappings.architectures import \
    BlimpIndexHitmapBankLayoutConfiguration, \
    BlimpHitmapBankLayoutConfiguration, \
    BlimpHitmapIndexBitweaveBankLayoutConfiguration, \
    BlimpHitmapRecordBitweaveBankLayoutConfiguration

from src.simulators.blimp import SimulatedBlimpBank


class _BlimpHitmapEquality(
    Query[
        SimulatedBlimpBank,
        Union[
            BlimpIndexHitmapBankLayoutConfiguration,
            BlimpHitmapBankLayoutConfiguration,
            BlimpHitmapIndexBitweaveBankLayoutConfiguration,
            BlimpHitmapRecordBitweaveBankLayoutConfiguration
        ]
    ]
):
    def _perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            negate: bool,
            return_labels: bool = False,
            hitmap_index: int = 0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a generic BLIMP EQUAL query operation and use a reserved row for BLIMP data

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param negate: Whether this is an EQUAL or NOTEQUAL operation
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        # Ensure the value is at least valid
        if value >= 2 ** (8 * pi_element_size_bytes):
            raise RuntimeError(f"This value is too large to be checking against {pi_element_size_bytes} byte indices")

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin(return_labels)

        # --------------------------------------------------------------------------------
        # The following will be injected into the bank at runtime, no timing penalties here
        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count
        hitmap_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index

        # How many records or rows are represented per row or record, respectively
        records_per_row = self.hardware.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_record_size_bytes
        rows_per_record = self.layout_configuration.database_configuration.total_record_size_bytes \
            // self.hardware.hardware_configuration.row_buffer_size_bytes

        # Simulator only, convert the value to bytes
        value_bytes = bitmanip.int_to_byte_array(value, pi_element_size_bytes)

        # --------------------------------------------------------------------------------

        # Algorithm bookkeeping
        runtime += self.simulator.blimp_cycle(5, "; initialization", return_labels)
        bitmap = 0
        bitdex = 0
        hitdex = 0
        current_row = 0

        # Iterate over all records
        runtime += self.simulator.blimp_cycle(1, "; loop start", return_labels)
        # Iterate over all record rows
        for r in range(self.layout_configuration.layout_metadata.total_records_processable):

            # Calculate the row and offset this record resides in
            if rows_per_record > 0:  # Multi-row per record
                row = self.layout_configuration.row_mapping.data[0] + r * rows_per_record
                offset = 0
            else:  # Multi-record per row
                row = self.layout_configuration.row_mapping.data[0] + r // records_per_row
                offset = r % records_per_row * self.layout_configuration.database_configuration.total_record_size_bytes
            sub_offset = offset + pi_subindex_offset_bytes

            runtime += self.simulator.blimp_cycle(3, "; row calculation", return_labels)
            runtime += self.simulator.blimp_cycle(3, "; offset calculation", return_labels)
            runtime += self.simulator.blimp_cycle(2, "; suboffset calculation", return_labels)

            # Do we need to fetch more data?
            runtime += self.simulator.blimp_cycle(3, "; row check", return_labels)
            if row != current_row:
                runtime += self.simulator.blimp_load_register(self.simulator.blimp_data_scratchpad, row, return_labels)
                current_row = row
            data = self.simulator.registers[self.simulator.blimp_data_scratchpad]

            # Perform the EQUAL via a byte memcmp
            runtime += self.simulator.blimp_cycle(pi_element_size_bytes * 2, "; memcmp", return_labels)
            equal = all(data[sub_offset + b] == value_bytes[b] for b in range(pi_element_size_bytes))

            # Update some metrics
            runtime += self.simulator.blimp_cycle(4, "; bookkeeping", return_labels)
            bitmap <<= 1
            bitmap += int(equal) if not negate else int(not equal)
            bitdex += 1

            # Manage full hitmaps
            if bitdex % 8 == 0:
                runtime += self.simulator.blimp_cycle(4, "; hitmap bookkeeping", return_labels)
                self.simulator.registers[self.simulator.blimp_v1][hitdex % self.hardware.hardware_configuration.row_buffer_size_bytes] = bitmap
                hitdex += 1
                bitmap = 0
                # Filled this register?
                if hitdex % self.hardware.hardware_configuration.row_buffer_size_bytes == 0:
                    runtime += self.simulator.blimp_save_register(self.simulator.blimp_v1, hitmap_base + ((hitdex - 1) // self.hardware.hardware_configuration.row_buffer_size_bytes))

        # All records are finished processing, save what we have now
        runtime += self.simulator.blimp_cycle(1, "; loop start", return_labels)
        while hitdex % self.hardware.hardware_configuration.row_buffer_size_bytes != 0:
            runtime += self.simulator.blimp_cycle(3, "; end bookkeeping", return_labels)
            bitmap <<= 1
            bitdex += 1
            if bitdex % 8 == 0:
                runtime += self.simulator.blimp_cycle(4, "; end hitmap bookkeeping", return_labels)
                self.simulator.registers[self.simulator.blimp_v1][hitdex % self.hardware.hardware_configuration.row_buffer_size_bytes] = bitmap
                hitdex += 1
                bitmap = 0
        runtime += self.simulator.blimp_save_register(self.simulator.blimp_v1, hitmap_base + ((hitdex - 1) // self.hardware.hardware_configuration.row_buffer_size_bytes))

        runtime += self.simulator.blimp_end(return_labels)

        # We have finished the query, fetch the hitmap to one single hitmap row
        hitmap_byte_array = []
        for h in range(rows_per_hitmap):
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index + h

            # Append the byte array for the next hitmap sub row
            hitmap_byte_array += self.hardware.get_row_bytes(hitmap_row)

        result = SimulationResult.from_hitmap_byte_array(
            hitmap_byte_array,
            self.layout_configuration.layout_metadata.total_records_processable
        )
        return runtime, result


class BlimpHitmapEqual(_BlimpHitmapEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a BLIMP EQUAL query operation. If the PI/Key field is segmented, specify the segment offset and its
        size, as well as the value to check against. The value must be less than the maximum size expressed by the
        provided size. Return debug labels if specified.

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        return self._perform_operation(
            pi_subindex_offset_bytes=pi_subindex_offset_bytes,
            pi_element_size_bytes=pi_element_size_bytes,
            value=value,
            negate=False,
            return_labels=return_labels,
            hitmap_index=hitmap_index
        )


class BlimpHitmapNotEqual(_BlimpHitmapEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a BLIMP NOTEQUAL query operation. If the PI/Key field is segmented, specify the segment offset and its
        size, as well as the value to check against. The value must be less than the maximum size expressed by the
        provided size. Return debug labels if specified.

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        return self._perform_operation(
            pi_subindex_offset_bytes=pi_subindex_offset_bytes,
            pi_element_size_bytes=pi_element_size_bytes,
            value=value,
            negate=True,
            return_labels=return_labels,
            hitmap_index=hitmap_index
        )
