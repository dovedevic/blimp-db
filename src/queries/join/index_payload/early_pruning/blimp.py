import math

from src.queries.query import Query
from src.simulators.result import RuntimeResult, MemoryArrayResult, HitmapResult
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.configurations.hashables import BlimpSimpleHashSet
from src.simulators.hardware import SimulatedBlimpBank
from src.utils.bitmanip import msb_bit


class BlimpHashmapEarlyPruningIndexPayloadJoin(
    Query[
        SimulatedBlimpBank,
        BlimpIndexHitmapBankLayoutConfiguration
    ]
):
    def perform_operation(
            self,
            hash_map: BlimpSimpleHashSet,
            output_array_start_row: int,
            output_index_size_bytes: int,
            hitmap_index: int = 0,
            **kwargs
    ) -> (RuntimeResult, MemoryArrayResult, HitmapResult):
        """
        Perform an BLIMP 32-bit Hash Probe operation on 32-bit keys. This assumes the database configuration
        parameter `total_index_size_bytes` is only referencing the entire key, not a multikey, and that the key is 32
        bits, or 4 bytes. Before a key is evaluated, we check the param:hitmap_index bit to see if this key should be
        evaluated. If it should be, we attempt to find a hit. When a hit is found the tuple (key,payload) is placed in
        an array starting at the :param:output_array_start_row memory address. If no hit was found the hitmap bit is
        flipped.

        @param hash_map: The hash set to be used for probing
        @param output_array_start_row: The row number where the output array begins
        @param output_index_size_bytes: The number of bytes to use for index hit values in the output array
        @param hitmap_index: Which hitmap to target results into and where to draw early termination results from
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

        # Ensure our memory bounds are valid
        hash_map_rows = math.ceil(
            hash_map.size // self.layout_configuration.hardware_configuration.row_buffer_size_bytes
        )
        assert self.layout_configuration.row_mapping.blimp_temp_region[1] - hash_map_rows > 0, \
            "No left over rows in the reserved space for output structures"
        assert self.layout_configuration.row_mapping.blimp_temp_region[0] + hash_map_rows <= output_array_start_row < (
                self.layout_configuration.row_mapping.blimp_temp_region[0] +
                self.layout_configuration.row_mapping.blimp_temp_region[1]
                ), "output_array_start_row is out of bounds from the defined temporary memory region"

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count

        hitmap_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index
        base_data_row = self.layout_configuration.row_mapping.data[0]
        base_hashmap_row = self.layout_configuration.row_mapping.blimp_temp_region[0]
        base_output_row = output_array_start_row
        current_output_row = base_output_row
        output_byte_index = 0
        hit_elements = 0
        current_hitmap_row = hitmap_base
        max_output_row = self.layout_configuration.row_mapping.blimp_temp_region[0] + \
            self.layout_configuration.row_mapping.blimp_temp_region[1]

        elements_processed = 0
        elements_per_row = self.layout_configuration.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_index_size_bytes
        assert elements_per_row > 0, "Total element size must be at least less than one row buffer"

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin(
        )

        # Calculate the above metadata
        runtime += self.simulator.blimp_cycle(
            cycles=15,
            label="; meta start",
        )

        # Clear a register for temporary output in V2
        runtime += self.simulator.blimp_set_register_to_zero(
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
                            register=self.simulator.blimp_v4,
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
                if hit:
                    runtime += self.simulator.blimp_cycle(
                        cycles=10,
                        label="; hit meta calculation",
                    )
                    rounded_index = (index + elements_per_row * d) & (2 ** (output_index_size_bytes * 8) - 1)
                    hit_value = (rounded_index << (hit.payload_type().size() * 8)) + hit.payload.as_int()
                    hit_size = output_index_size_bytes + hit.payload_type().size()
                    original_hit_size = hit_size
                    hit_elements += 1

                    while hit_size > 0:
                        # partially save what we can
                        bytes_remaining = self.hardware.hardware_configuration.row_buffer_size_bytes - output_byte_index
                        placeable_bytes = min(
                            hit_size,
                            bytes_remaining
                        )  # we want to assert the condition that placeable_bytes is always at least > 0

                        if placeable_bytes < hit_size:
                            mask = (2 ** (placeable_bytes * 8) - 1) << ((hit_size - placeable_bytes) * 8)
                            inserted_value = (hit_value & mask) >> ((hit_size - placeable_bytes) * 8)
                        else:
                            inserted_value = hit_value

                        runtime += self.simulator.blimp_set_register_data_at_index(
                            register=self.simulator.blimp_v2,
                            element_width=placeable_bytes,
                            index=output_byte_index // original_hit_size,
                            value=inserted_value,
                        )
                        output_byte_index += placeable_bytes

                        hit_value &= (2 ** ((hit_size - placeable_bytes) * 8)) - 1
                        hit_size -= placeable_bytes

                        runtime += self.simulator.blimp_cycle(
                            cycles=2,
                            label="; hit state check",
                        )
                        if output_byte_index >= self.hardware.hardware_configuration.row_buffer_size_bytes:
                            if current_output_row >= max_output_row:
                                raise RuntimeError("maximum output memory exceeded")

                            # try to save the output buffer
                            runtime += self.simulator.blimp_save_register(
                                register=self.simulator.blimp_v2,
                                row=current_output_row,
                            )
                            runtime += self.simulator.blimp_set_register_to_zero(
                                register=self.simulator.blimp_v2,
                            )
                            current_output_row += 1
                            output_byte_index = 0
                else:  # update the hitmap to reflect the non-hit
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

        # were done with records processing, but we need to save one last time possibly
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; cmp save",
        )
        if output_byte_index != 0:
            if current_output_row >= max_output_row:
                raise RuntimeError("maximum output memory exceeded")

            # save the output buffer
            runtime += self.simulator.blimp_save_register(
                register=self.simulator.blimp_v2,
                row=current_output_row,
            )
            current_output_row += 1

        runtime += self.simulator.blimp_save_register(
            register=self.simulator.blimp_v3,
            row=current_hitmap_row,
        )

        runtime += self.simulator.blimp_end()

        # We have finished the query, fetch the memory array to one single array
        memory_byte_array = []
        for r in range(current_output_row - base_output_row):
            # Append the byte array for the next hitmap sub row
            memory_byte_array += self.simulator.bank_hardware.get_row_bytes(base_output_row + r)

        class IndexPayloadResult(hash_map.bucket_type().bucket_object_type()):
            class IndexHit(hash_map.bucket_type().bucket_object_type().key_type()):
                _SIZE_BYTES = output_index_size_bytes
            _KEY_OBJECT = IndexHit

        memory_result = MemoryArrayResult.from_byte_array(
            byte_array=memory_byte_array[
                0:(
                    output_index_size_bytes + hash_map.bucket_type().bucket_object_type().payload_type().size()
                ) * hit_elements
            ],
            element_width=output_index_size_bytes + hash_map.bucket_type().bucket_object_type().payload_type().size(),
            cast_as=IndexPayloadResult.from_int
        )

        # We have finished the query, fetch the hitmap to one single hitmap row
        hitmap_byte_array = []
        for h in range(rows_per_hitmap):
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index + h

            # Append the byte array for the next hitmap sub row
            hitmap_byte_array += self.simulator.bank_hardware.get_row_bytes(hitmap_row)

        hitmap_result = HitmapResult.from_hitmap_byte_array(
            hitmap_byte_array=hitmap_byte_array,
            num_bits=self.layout_configuration.layout_metadata.total_records_processable
        )

        return runtime, memory_result, hitmap_result
