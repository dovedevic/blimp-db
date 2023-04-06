import math

from typing import Union

from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.configurations.hashables import BlimpSimpleHashSet

from src.simulators import SimulatedBlimpVBank


class BlimpVHashmapJoin(
    Query[
        SimulatedBlimpVBank,
        Union[
            BlimpIndexHitmapBankLayoutConfiguration
        ]
    ]
):
    def perform_operation(
            self,
            hash_set: BlimpSimpleHashSet,
            return_labels: bool=False,
            hitmap_index: int = 0,
            **kwargs
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform an BLIMP-V 32-bit Hash Probe operation on 32-bit keys. This assumes the database configuration
        parameter `total_index_size_bytes` is only referencing the entire key, not a multikey, and that the key is 32
        bits, or 4 bytes.

        @param hash_set: The hash set to be used for probing
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        key_size = self.layout_configuration.database_configuration.total_index_size_bytes
        assert key_size == 4, "This implementation of Hash Probe expects keys to be 4 bytes / 32 bits"

        assert hash_set.size <= self.layout_configuration.database_configuration.blimp_temporary_region_size_bytes, \
               "There is not enough temporary space allocated for the maximum size of this hash table"

        # Ensure we have enough hitmaps to index into
        if hitmap_index >= self.layout_configuration.database_configuration.hitmap_count:
            raise RuntimeError(
                f"The provided hitmap index {hitmap_index} is out of bounds. The current configuration "
                f"only supports {self.layout_configuration.database_configuration.hitmap_count} hitmaps")

        # Ensure we have a fresh set of hitmaps
        self.layout_configuration.reset_hitmap_index_to_value(self.hardware, True, hitmap_index)

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count

        hitmap_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index
        base_data_row = self.layout_configuration.row_mapping.data[0]
        base_hashmap_row = self.layout_configuration.row_mapping.blimp_temp_region[0]

        elements_processed = 0
        elements_per_row = self.layout_configuration.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_index_size_bytes
        assert elements_per_row > 0, "Total element size must be at least less than one row buffer"

        # Begin by enabling BLIMP-V
        runtime = self.simulator.blimp_begin(return_labels)

        # Calculate the above metadata
        runtime += self.simulator.blimp_cycle(5, "; meta start", return_labels)

        # Clear a register for temporary hitmaps in V2
        runtime += self.simulator.blimpv_set_register_to_zero(self.simulator.blimp_v2, return_labels)

        # Iterate over all data rows
        runtime += self.simulator.blimp_cycle(3, "; loop start", return_labels)
        for d in range(self.layout_configuration.row_mapping.data[1]):

            runtime += self.simulator.blimp_cycle(1, "; data row calculation", return_labels)
            data_row = base_data_row + d

            # Load in elements_per_row elements into the vector registers. DS is for keys, V1 is for hash(keys)
            runtime += self.simulator.blimp_load_register(
                self.simulator.blimp_data_scratchpad, data_row, return_labels
            )
            runtime += self.simulator.blimp_transfer_register(
                self.simulator.blimp_data_scratchpad, self.simulator.blimp_v1, return_labels
            )

            # Hash and mask the keys
            runtime += self.simulator.blimpv_alu_int_hash(
                self.simulator.blimp_v1,
                key_size,
                key_size,
                hash_set.mask,
                return_labels=return_labels
            )

            # Loop through them searching for hits
            # TODO: Possible optimization, perform a search for all matching hash-indices in this row
            current_row_index = -1
            runtime += self.simulator.blimp_cycle(3, "; row loop start", return_labels)
            for index, key in enumerate(self.simulator.blimp_get_register_data(
                    self.simulator.blimp_data_scratchpad,
                    self.layout_configuration.database_configuration.total_index_size_bytes)):

                if elements_processed + index >= self.layout_configuration.layout_metadata.total_records_processable:
                    break

                traced_buckets, traced_iterations, hit = hash_set.traced_fetch(key)

                # Add the timings to check the hit
                for traced_bucket, traced_iteration in zip(traced_buckets, traced_iterations):
                    # Check if the blimp memory control needs to fetch a row
                    traced_row_index = traced_bucket // \
                        (self.hardware.hardware_configuration.row_buffer_size_bytes // hash_set.bucket_type().size())
                    runtime += self.simulator.blimp_cycle(1, "; register address check", return_labels)
                    if current_row_index != traced_row_index:
                        current_row_index = traced_row_index
                        runtime += self.simulator.blimp_load_register(
                            self.simulator.blimp_v3,
                            base_hashmap_row + current_row_index,
                            return_labels=return_labels
                        )

                    # Use the vector register to perform several equality checks at once in the bucket
                    cycles = 1  # Start with one cycle to dispatch to the vector engine
                    elements_to_check = hash_set.bucket_type().bucket_capacity()
                    operable_alus = self.hardware.hardware_configuration.number_of_vALUs
                    alu_rounds = int(math.ceil(elements_to_check / operable_alus))
                    cycles += alu_rounds  # perform == on all elements wrt hash(key)
                    cycles += 1           # check v3 ZERO register
                    cycles += 1           # jump, depending on answer

                    runtime += self.simulator.blimp_cycle(cycles, return_labels=return_labels)

                # set the hit
                runtime += self.simulator.blimp_set_register_data_at_index(
                    self.simulator.blimp_v1,
                    self.layout_configuration.database_configuration.total_index_size_bytes,
                    index,
                    +(hit is not None)
                )

            # Coalesce the bitmap
            runtime += self.simulator.blimpv_coalesce_register_hitmap(
                self.simulator.blimp_v1,
                self.layout_configuration.database_configuration.total_index_size_bytes,
                self.layout_configuration.database_configuration.total_index_size_bytes,
                elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8),
                return_labels
            )

            # Or the bitmap into the temporary one
            runtime += self.simulator.blimpv_alu_int_or(
                self.simulator.blimp_v1,
                self.simulator.blimp_v2,
                self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                return_labels
            )

            runtime += self.simulator.blimp_cycle(1, "; metadata calculation", return_labels)
            elements_processed = min(
                elements_processed + elements_per_row,
                self.layout_configuration.layout_metadata.total_records_processable
            )

            # do we need to reset?
            runtime += self.simulator.blimp_cycle(3, "; cmp elements processed", return_labels)
            if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) == 0:

                # Save the hitmap
                runtime += self.simulator.blimp_save_register(
                    self.simulator.blimp_v2,
                    hitmap_base +
                    (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8))
                    - 1,
                    return_labels
                )

                # Reset to save a new one
                runtime += self.simulator.blimpv_set_register_to_zero(self.simulator.blimp_v2, return_labels)

            runtime += self.simulator.blimp_cycle(2, "; loop return", return_labels)

        # were done with records processing, but we need to save one last time possibly
        runtime += self.simulator.blimp_cycle(3, "; cmp save", return_labels)
        if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) != 0:
            runtime += self.simulator.blimp_save_register(
                self.simulator.blimp_v2,
                hitmap_base +
                (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)),
                return_labels
            )

        runtime += self.simulator.blimp_end(return_labels)

        # Do we need to pad off remaining hits? This will be handled already by us with V-ASM but we need to do it here
        remainder = elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)
        null_remainder = self.hardware.hardware_configuration.row_buffer_size_bytes * 8 - remainder
        segmented_row = ((2 ** remainder) - 1) << null_remainder

        raw_row = self.hardware.get_raw_row(
            hitmap_base + (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8))
        )
        new_raw_row = raw_row & segmented_row

        self.hardware.set_raw_row(
            hitmap_base + (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)),
            new_raw_row
        )

        # We have finished the query, fetch the hitmap to one single hitmap row
        hitmap_byte_array = []
        for h in range(rows_per_hitmap):
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index + h

            # Append the byte array for the next hitmap sub row
            hitmap_byte_array += self.simulator.bank_hardware.get_row_bytes(hitmap_row)

        result = HitmapResult.from_hitmap_byte_array(
            hitmap_byte_array,
            self.layout_configuration.layout_metadata.total_records_processable
        )
        return runtime, result
