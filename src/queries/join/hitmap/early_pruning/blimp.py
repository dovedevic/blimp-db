from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.configurations.hashables import BlimpSimpleHashSet
from src.simulators.hardware import SimulatedBlimpBank
from src.utils.bitmanip import msb_bit


class BlimpHashmapEarlyPruningJoin(
    Query[
        SimulatedBlimpBank,
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
        Perform an BLIMP 32-bit Hash Probe operation on 32-bit keys. This assumes the database configuration
        parameter `total_index_size_bytes` is only referencing the entire key, not a multikey, and that the key is 32
        bits, or 4 bytes. Before a key is evaluated however, we check the existing hitmap to see if the bit is on to
        indicate if this key should be checked or evaluated. If no hit was found when the bit was set, we toggle the
        output hitmap bit.

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
        current_hitmap_row = hitmap_base
        elements_per_row = self.layout_configuration.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_index_size_bytes
        assert elements_per_row > 0, "Total element size must be at least less than one row buffer"

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin(
        )

        # Calculate the above metadata
        runtime += self.simulator.blimp_cycle(
            cycles=5,
            label="; meta start",
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

            runtime += self.simulator.blimp_cycle(
                cycles=5,
                label="; mask calculation",
            )
            if elements_processed % (self.layout_configuration.hardware_configuration.row_buffer_size_bytes * 8) == 0:
                if elements_processed != 0:
                    runtime += self.simulator.blimp_save_register(
                        register=self.simulator.blimp_v3,
                        row=current_hitmap_row,
                    )
                    runtime += self.simulator.blimp_cycle(
                        cycles=1,
                        label="; hitmap bump",
                    )
                    current_hitmap_row += 1
                runtime += self.simulator.blimp_load_register(
                    register=self.simulator.blimp_v3,
                    row=current_hitmap_row,
                )
            runtime += self.simulator.blimp_cycle(
                cycles=5,
                label="; mask point",
            )
            mask = self.simulator.blimp_get_register_data(
                register=self.simulator.blimp_v3,
                element_width=elements_per_row // 8
            )[
                (elements_processed % (self.layout_configuration.hardware_configuration.row_buffer_size_bytes * 8)) //
                elements_per_row
            ]

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
            runtime += self.simulator.blimp_alu_int_hash(
                register_a=self.simulator.blimp_v1,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=key_size,
                stride=key_size,
                hash_mask=hash_map.mask,
                mask=mask,
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
                    break

                # check if this index was masked out
                # super hacky: this cycle count is included in the hash call since ideally these would be done together
                # like so: for key in keys... if bit { hash(key) traverse(hash) } else { next }
                if not msb_bit(mask, index, elements_per_row):
                    continue

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
                            register=self.simulator.blimp_v2,
                            row=base_hashmap_row + current_row_index,
                        )

                    # add iterations * 2 for cmp/jmp on keys
                    runtime += self.simulator.blimp_cycle(
                        cycles=max(1, traced_iteration * 2),
                    )

                # set the hit
                runtime += self.simulator.blimp_cycle(
                    cycles=1,
                    label="; hit check",
                )
                if not hit:  # update the hitmap to reflect the non-hit
                    runtime += self.simulator.blimp_cycle(
                        cycles=2,
                        label="; bit flip",
                    )
                    bit_flip = 1 << (elements_per_row - index - 1)
                    mask ^= bit_flip
                    runtime += self.simulator.blimp_set_register_data_at_index(  # TODO: Optimize?
                        register=self.simulator.blimp_v3,
                        element_width=elements_per_row // 8,
                        index=(
                                  elements_processed % (
                                      self.layout_configuration.hardware_configuration.row_buffer_size_bytes * 8
                                  )
                              ) // elements_per_row,
                        value=mask,
                        assume_one_cycle=True  # we would only save one byte at a time but this gets the job done enough
                    )

            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; metadata calculation",
            )
            elements_processed = min(
                elements_processed + elements_per_row,
                self.layout_configuration.layout_metadata.total_records_processable
            )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
            )

        # were done with records processing, but we need to save one last time
        runtime += self.simulator.blimp_save_register(
            register=self.simulator.blimp_v3,
            row=current_hitmap_row,
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
