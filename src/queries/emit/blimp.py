from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult, MemoryArrayResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.configurations.hardware.blimp import BlimpHardwareConfiguration
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures.blimp import BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
from src.simulators.hardware import SimulatedBlimpBank


class BlimpHitmapEmit(
    Query[
        SimulatedBlimpBank,
        DataLayoutConfiguration[
            BlimpHardwareConfiguration, BlimpHitmapDatabaseConfiguration,
            BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
        ]
    ]
):
    def perform_operation(
            self,
            hitmap_index: int,
            output_array_start_row: int,
            return_labels: bool=False,
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP hitmap emit operation on a provided hitmap index.

        @param hitmap_index: Which hitmap to target for bit counting
        @param output_array_start_row: The row number where the output array begins
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        """

        # Ensure we are operating on valid hitmap indices
        if hitmap_index >= self.layout_configuration.database_configuration.hitmap_count:
            raise RuntimeError(f"The hitmap index {hitmap_index} is out of bounds. The current configuration "
                               f"only supports {self.layout_configuration.database_configuration.hitmap_count} hitmaps")

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count

        hitmap_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index
        base_data_row = self.layout_configuration.row_mapping.data[0]
        base_output_row = output_array_start_row
        current_data_row = 0
        current_output_row = base_output_row
        output_byte_index = 0
        hit_elements = 0
        elements_processed = 0
        max_output_row = self.layout_configuration.row_mapping.blimp_temp_region[0] + \
            self.layout_configuration.row_mapping.blimp_temp_region[1]

        elements_per_row = self.layout_configuration.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_index_size_bytes
        assert elements_per_row > 0, "Total element size must be at least less than one row buffer"

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin(
            return_labels=return_labels
        )

        # Calculate the above metadata
        runtime += self.simulator.blimp_cycle(
            cycles=10,
            label="; meta start",
            return_labels=return_labels
        )

        # Clear a register for temporary output in V2
        runtime += self.simulator.blimp_set_register_to_zero(
            register=self.simulator.blimp_v2,
            return_labels=return_labels
        )

        # Iterate over all hitmap rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
            return_labels=return_labels
        )
        for h in range(rows_per_hitmap):
            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; hitmap row calculation",
                return_labels=return_labels
            )
            # Calculate the hitmap we are targeting
            hitmap_row = hitmap_base + h

            # move hitmap[index] into v1
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_v1,
                row=hitmap_row,
                return_labels=return_labels
            )

            # iterate over all the bytes
            runtime += self.simulator.blimp_cycle(
                cycles=3,
                label="; inner loop start",
                return_labels=return_labels
            )
            for byte_index, hitmap_byte in enumerate(self.simulator.blimp_get_register_data(
                    register=self.simulator.blimp_v1, element_width=1)):

                if elements_processed >= self.layout_configuration.layout_metadata.total_records_processable:
                    break

                # check if this byte can be skipped
                runtime += self.simulator.blimp_cycle(
                    cycles=1,
                    label="; early stop",
                    return_labels=return_labels
                )
                if hitmap_byte == 0:
                    elements_processed += 8
                    continue

                # check for hit bits
                for i in range(8):
                    elements_processed += 1
                    if elements_processed > self.layout_configuration.layout_metadata.total_records_processable:
                        break

                    runtime += self.simulator.blimp_cycle(
                        cycles=2,
                        label="; hit check",
                        return_labels=return_labels
                    )
                    if hitmap_byte & (1 << (7 - i)):
                        hit_elements += 1

                        # calculate a bank-relative record index
                        runtime += self.simulator.blimp_cycle(
                            cycles=3,
                            label="; record index calculation",
                            return_labels=return_labels
                        )
                        record_index = \
                            h * self.layout_configuration.hardware_configuration.row_buffer_size_bytes * 8 + \
                            byte_index * 8 + i

                        # calculate the row and row-index of this record in the data segment
                        record_row = base_data_row + \
                            record_index * \
                            self.layout_configuration.database_configuration.total_index_size_bytes // \
                            self.layout_configuration.hardware_configuration.row_buffer_size_bytes
                        record_row_index = record_index % elements_per_row

                        # load the record if it isn't already loaded
                        runtime += self.simulator.blimp_cycle(
                            cycles=1,
                            label="; register address check",
                            return_labels=return_labels
                        )
                        if record_row != current_data_row:
                            # load the row
                            runtime += self.simulator.blimp_load_register(
                                register=self.simulator.blimp_v3,
                                row=record_row,
                                return_labels=return_labels
                            )
                            current_data_row = record_row

                        # fetch the data associated with this bit
                        runtime += self.simulator.blimp_cycle(
                            cycles=10,
                            label="; emit meta calculation",
                            return_labels=return_labels
                        )
                        emit_value = self.simulator.blimp_get_register_data(
                            register=self.simulator.blimp_v3,
                            element_width=self.layout_configuration.database_configuration.total_index_size_bytes
                        )[record_row_index]
                        emit_size = self.layout_configuration.database_configuration.total_index_size_bytes
                        while emit_size > 0:
                            # partially save what we can
                            bytes_remaining = self.hardware.hardware_configuration.row_buffer_size_bytes - \
                                              output_byte_index
                            placeable_bytes = min(
                                emit_size,
                                bytes_remaining
                            )  # we want to assert the condition that placeable_bytes is always at least > 0

                            if placeable_bytes < emit_size:
                                mask = (2 ** (placeable_bytes * 8) - 1) << ((emit_size - placeable_bytes) * 8)
                                inserted_value = (emit_value & mask) >> ((emit_size - placeable_bytes) * 8)
                            else:
                                inserted_value = emit_value

                            runtime += self.simulator.blimp_set_register_data_at_index(
                                register=self.simulator.blimp_v2,
                                element_width=placeable_bytes,
                                index=output_byte_index //
                                self.layout_configuration.database_configuration.total_index_size_bytes,
                                value=inserted_value,
                                return_labels=return_labels
                            )
                            output_byte_index += placeable_bytes

                            emit_value &= (2 ** ((emit_size - placeable_bytes) * 8)) - 1
                            emit_size -= placeable_bytes

                            runtime += self.simulator.blimp_cycle(
                                cycles=2,
                                label="; emit state check",
                                return_labels=return_labels
                            )
                            if output_byte_index >= self.hardware.hardware_configuration.row_buffer_size_bytes:
                                if current_output_row >= max_output_row:
                                    raise RuntimeError("maximum output memory exceeded")

                                # try to save the output buffer
                                runtime += self.simulator.blimp_save_register(
                                    register=self.simulator.blimp_v2,
                                    row=current_output_row,
                                    return_labels=return_labels
                                )
                                runtime += self.simulator.blimp_set_register_to_zero(
                                    register=self.simulator.blimp_v2,
                                    return_labels=return_labels
                                )
                                current_output_row += 1
                                output_byte_index = 0

                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="; inner loop return",
                    return_labels=return_labels
                )
            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
                return_labels=return_labels
            )

        # were done with records processing, but we need to save one last time possibly
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; cmp save",
            return_labels=return_labels
        )
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

        runtime += self.simulator.blimp_end(return_labels=return_labels)

        # We have finished the query, fetch the memory array to one single array
        memory_byte_array = []
        for r in range(current_output_row - base_output_row):
            # Append the byte array for the next hitmap sub row
            memory_byte_array += self.simulator.bank_hardware.get_row_bytes(base_output_row + r)

        memory_result = MemoryArrayResult.from_byte_array(
            byte_array=memory_byte_array[
                0:(self.layout_configuration.database_configuration.total_index_size_bytes * hit_elements)
            ],
            element_width=self.layout_configuration.database_configuration.total_index_size_bytes,
            cast_as=int
        )

        return runtime, memory_result
