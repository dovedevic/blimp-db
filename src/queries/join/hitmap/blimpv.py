import math

from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.configurations.hashables import BlimpSimpleHashSet
from src.simulators.hardware import SimulatedBlimpVBank


class BlimpVHashmapJoin(
    Query[
        SimulatedBlimpVBank,
        BlimpIndexHitmapBankLayoutConfiguration
    ]
):
    def perform_operation(
            self,
            hash_map: BlimpSimpleHashSet,
            hitmap_index: int = 0,
            **kwargs
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform an BLIMP-V 32-bit Hash Probe operation on 32-bit keys. This assumes the database configuration
        parameter `total_index_size_bytes` is only referencing the entire key, not a multikey, and that the key is 32
        bits, or 4 bytes.

        @param hash_map: The hash map to be used for probing
        @param hitmap_index: Which hitmap to target results into
        """
        key_size = self.layout_configuration.database_configuration.total_index_size_bytes
        assert key_size == 4, "This implementation of Hash Probe expects keys to be 4 bytes / 32 bits"

        assert hash_map.size <= self.layout_configuration.database_configuration.blimp_temporary_region_size_bytes, \
               "There is not enough temporary space allocated for the maximum size of this hash table"

        # Ensure we have enough hitmaps to index into
        if hitmap_index >= self.layout_configuration.database_configuration.hitmap_count:
            raise RuntimeError(
                f"The provided hitmap index {hitmap_index} is out of bounds. The current configuration "
                f"only supports {self.layout_configuration.database_configuration.hitmap_count} hitmaps")

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
        runtime = self.simulator.blimp_begin()

        # Calculate the above metadata
        runtime += self.simulator.blimp_cycle(
            cycles=5,
            label="; meta start",
        )

        # Clear a register for temporary hitmaps in V2
        runtime += self.simulator.blimpv_set_register_to_zero(
            register=self.simulator.blimp_v2,
        )

        # Iterate over all data rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
        )
        for d in range(self.layout_configuration.row_mapping.data[1]):

            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; data row calculation",
            )
            data_row = base_data_row + d

            # Load in elements_per_row elements into the vector registers. DS is for keys, V1 is for hash(keys)
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_data_scratchpad,
                row=data_row,
            )
            runtime += self.simulator.blimp_transfer_register(
                register_a=self.simulator.blimp_data_scratchpad,
                register_b=self.simulator.blimp_v1,
            )

            # Hash and mask the keys
            runtime += self.simulator.blimpv_alu_int_hash(
                register_a=self.simulator.blimp_v1,
                sew=key_size,
                stride=key_size,
                hash_mask=hash_map.mask,
            )

            # Loop through them searching for hits
            # TODO: Possible optimization, perform a search for all matching hash-indices in this row
            current_row_index = -1
            runtime += self.simulator.blimp_cycle(
                cycles=3,
                label="; row loop start",
            )
            for index, key in enumerate(self.simulator.blimp_get_register_data(
                    register=self.simulator.blimp_data_scratchpad,
                    element_width=self.layout_configuration.database_configuration.total_index_size_bytes)):

                if elements_processed + index >= self.layout_configuration.layout_metadata.total_records_processable:
                    # The rest of the hitmap must be zeroed out
                    for i in range(index, elements_per_row):
                        self.simulator.blimp_set_register_data_at_index(
                            register=self.simulator.blimp_v1,
                            element_width=self.layout_configuration.database_configuration.total_index_size_bytes,
                            index=i,
                            value=0,
                        )
                    break

                traced_buckets, traced_iterations, hit = hash_map.traced_fetch(key)

                # Add the timings to check the hit
                for traced_bucket, traced_iteration in zip(traced_buckets, traced_iterations):
                    # Check if the blimp memory control needs to fetch a row
                    traced_row_index = traced_bucket // \
                        (self.hardware.hardware_configuration.row_buffer_size_bytes // hash_map.bucket_type().size())
                    runtime += self.simulator.blimp_cycle(
                        cycles=1,
                        label="; register address check",
                    )
                    if current_row_index != traced_row_index:
                        current_row_index = traced_row_index
                        runtime += self.simulator.blimp_load_register(
                            register=self.simulator.blimp_v3,
                            row=base_hashmap_row + current_row_index,
                        )

                    # Use the vector register to perform several equality checks at once in the bucket
                    cycles = 1  # Start with one cycle to dispatch to the vector engine
                    elements_to_check = hash_map.bucket_type().bucket_capacity()
                    operable_alus = self.hardware.hardware_configuration.number_of_vALUs
                    alu_rounds = int(math.ceil(elements_to_check / operable_alus))
                    cycles += alu_rounds  # perform == on all elements wrt hash(key)
                    cycles += 1           # check v3 ZERO register
                    cycles += 1           # jump, depending on answer

                    runtime += self.simulator.blimp_cycle(
                        cycles=cycles,
                    )

                # set the hit
                runtime += self.simulator.blimp_set_register_data_at_index(
                    register=self.simulator.blimp_v1,
                    element_width=self.layout_configuration.database_configuration.total_index_size_bytes,
                    index=index,
                    value=+(hit is not None),
                )

            # Coalesce the bitmap, no need to save the runtime since ideally we would do this while looping when we
            # do the above ALU operations. We do this here just to do it handily with the sim
            self.simulator.blimpv_coalesce_register_hitmap(
                register_a=self.simulator.blimp_v1,
                sew=self.layout_configuration.database_configuration.total_index_size_bytes,
                stride=self.layout_configuration.database_configuration.total_index_size_bytes,
                bit_offset=(d * elements_per_row) % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8),
            )

            # Or the bitmap into the temporary one, no runtime for the same reason as above
            self.simulator.blimpv_alu_int_or(
                register_a=self.simulator.blimp_v1,
                register_b=self.simulator.blimp_v2,
                sew=self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                stride=self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
            )

            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; metadata calculation",
            )
            elements_processed = min(
                elements_processed + elements_per_row,
                self.layout_configuration.layout_metadata.total_records_processable
            )

            # do we need to reset?
            runtime += self.simulator.blimp_cycle(
                cycles=3,
                label="; cmp elements processed",
            )
            if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) == 0:

                # Save the hitmap
                runtime += self.simulator.blimp_save_register(
                    register=self.simulator.blimp_v2,
                    row=hitmap_base +
                    (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)) - 1,
                )

                # Reset to save a new one
                runtime += self.simulator.blimpv_set_register_to_zero(
                    register=self.simulator.blimp_v2,
                )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
            )

        # were done with records processing, but we need to save one last time possibly
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; cmp save",
        )
        if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) != 0:
            runtime += self.simulator.blimp_save_register(
                register=self.simulator.blimp_v2,
                row=hitmap_base +
                (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)),
            )

        runtime += self.simulator.blimp_end()

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
            hitmap_byte_array=hitmap_byte_array,
            num_bits=self.layout_configuration.layout_metadata.total_records_processable
        )
        return runtime, result
