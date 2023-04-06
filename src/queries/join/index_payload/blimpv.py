import math

from src.queries.query import Query
from src.simulators.result import RuntimeResult, MemoryArrayResult
from src.data_layout_mappings.architectures import BlimpIndexBankLayoutConfiguration
from src.configurations.hashables import BlimpSimpleHashSet
from src.simulators import SimulatedBlimpVBank


class BlimpVHashmapIndexPayloadJoin(
    Query[
        SimulatedBlimpVBank,
        BlimpIndexBankLayoutConfiguration
    ]
):
    def perform_operation(
            self,
            hash_map: BlimpSimpleHashSet,
            output_array_start_row: int,
            output_index_size_bytes: int,
            return_labels: bool=False,
            **kwargs
    ) -> (RuntimeResult, MemoryArrayResult):
        """
        Perform an BLIMP-V 32-bit Hash Probe operation on 32-bit keys. This assumes the database configuration
        parameter `total_index_size_bytes` is only referencing the entire key, not a multikey, and that the key is 32
        bits, or 4 bytes. When a hit is found, instead of using a hitmap, the tuple (key,payload) is placed in an array
        starting at the :param:output_array_start_row memory address.

        @param hash_map: The hash set to be used for probing
        @param output_array_start_row: The row number where the output array begins
        @param output_index_size_bytes: The number of bytes to use for index hit values in the output array
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        """
        key_size = self.layout_configuration.database_configuration.total_index_size_bytes
        assert key_size == 4, "This implementation of Hash Probe expects keys to be 4 bytes / 32 bits"

        assert hash_map.size <= self.layout_configuration.database_configuration.blimp_temporary_region_size_bytes, \
               "There is not enough temporary space allocated for the maximum size of this hash table"

        # Ensure our memory bounds are valid
        hash_map_rows = math.ceil(
            hash_map.size // self.layout_configuration.hardware_configuration.row_buffer_size_bytes
        )
        assert self.layout_configuration.row_mapping.blimp_temp_region[1] - hash_map_rows > 0, \
            "No left over rows in the reserved space for output structures"
        assert self.layout_configuration.row_mapping.blimp_temp_region[0] + hash_map_rows < output_array_start_row < (
                self.layout_configuration.row_mapping.blimp_temp_region[0] +
                self.layout_configuration.row_mapping.blimp_temp_region[1]
                ), "output_array_start_row is out of bounds from the defined temporary memory region"

        base_data_row = self.layout_configuration.row_mapping.data[0]
        base_hashmap_row = self.layout_configuration.row_mapping.blimp_temp_region[0]
        base_output_row = output_array_start_row
        current_output_row = base_output_row
        output_byte_index = 0
        hit_elements = 0
        max_output_row = self.layout_configuration.row_mapping.blimp_temp_region[0] + \
            self.layout_configuration.row_mapping.blimp_temp_region[1]

        elements_processed = 0
        elements_per_row = self.layout_configuration.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_index_size_bytes
        assert elements_per_row > 0, "Total element size must be at least less than one row buffer"

        # Begin by enabling BLIMP-V
        runtime = self.simulator.blimp_begin(return_labels)

        # Calculate the above metadata
        runtime += self.simulator.blimp_cycle(10, "; meta start", return_labels)

        # Clear a register for temporary output in V2
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
                hash_map.mask,
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

                traced_buckets, traced_iterations, hit = hash_map.traced_fetch(key)

                # Add the timings to check the hit
                for traced_bucket, traced_iteration in zip(traced_buckets, traced_iterations):
                    # Check if the blimp memory control needs to fetch a row
                    traced_row_index = traced_bucket // \
                        (self.hardware.hardware_configuration.row_buffer_size_bytes // hash_map.bucket_type().size())
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
                        elements_to_check = hash_map.bucket_type().bucket_capacity()
                        operable_alus = self.hardware.hardware_configuration.number_of_vALUs
                        alu_rounds = int(math.ceil(elements_to_check / operable_alus))
                        cycles += alu_rounds  # perform == on all elements wrt hash(key)
                        cycles += 1  # check v3 ZERO register
                        cycles += 1  # jump, depending on answer

                        runtime += self.simulator.blimp_cycle(cycles, return_labels=return_labels)

                # set the hit
                runtime += self.simulator.blimp_cycle(1, "; hit check", return_labels)
                if hit:
                    runtime += self.simulator.blimp_cycle(10, "; hit meta calculation", return_labels)
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
                            return_labels=return_labels
                        )
                        output_byte_index += placeable_bytes

                        hit_value &= (2 ** ((hit_size - placeable_bytes) * 8)) - 1
                        hit_size -= placeable_bytes

                        runtime += self.simulator.blimp_cycle(2, "; hit state check", return_labels)
                        if output_byte_index >= self.hardware.hardware_configuration.row_buffer_size_bytes:
                            if current_output_row <= max_output_row:
                                raise RuntimeError("maximum output memory exceeded")

                            # try to save the output buffer
                            runtime += self.simulator.blimp_save_register(
                                register=self.simulator.blimp_v2,
                                row=current_output_row,
                                return_labels=return_labels
                            )
                            runtime += self.simulator.blimpv_set_register_to_zero(
                                self.simulator.blimp_v2, return_labels
                            )
                            current_output_row += 1
                            output_byte_index = 0

            runtime += self.simulator.blimp_cycle(1, "; metadata calculation", return_labels)
            elements_processed = min(
                elements_processed + elements_per_row,
                self.layout_configuration.layout_metadata.total_records_processable
            )

            runtime += self.simulator.blimp_cycle(2, "; loop return", return_labels)

        # were done with records processing, but we need to save one last time possibly
        runtime += self.simulator.blimp_cycle(3, "; cmp save", return_labels)
        if output_byte_index != 0:
            if current_output_row >= max_output_row:
                raise RuntimeError("maximum output memory exceeded")

            # save the output buffer
            runtime += self.simulator.blimp_save_register(
                register=self.simulator.blimp_v2,
                row=current_output_row,
                return_labels=return_labels
            )
            current_output_row += 1

        runtime += self.simulator.blimp_end(return_labels)

        # We have finished the query, fetch the memory array to one single array
        memory_byte_array = []
        for r in range(current_output_row - base_output_row):
            # Append the byte array for the next hitmap sub row
            memory_byte_array += self.simulator.bank_hardware.get_row_bytes(base_output_row + r)

        class IndexPayloadResult(hash_map.bucket_type().bucket_object_type()):
            class IndexHit(hash_map.bucket_type().bucket_object_type().key_type()):
                _SIZE_BYTES = output_index_size_bytes
            _KEY_OBJECT = IndexHit

        result = MemoryArrayResult.from_byte_array(
            memory_byte_array[
                0:(
                    output_index_size_bytes + hash_map.bucket_type().bucket_object_type().payload_type().size()
                ) * hit_elements
            ],
            output_index_size_bytes + hash_map.bucket_type().bucket_object_type().payload_type().size(),
            cast_as=IndexPayloadResult.from_int
        )
        return runtime, result
