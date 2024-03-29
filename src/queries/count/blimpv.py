from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration
from src.configurations.database.blimp import BlimpVectorHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures.blimp import BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
from src.utils.bitmanip import byte_array_to_int
from src.simulators.hardware import SimulatedBlimpVBank


class BlimpVHitmapCount(
    Query[
        SimulatedBlimpVBank,
        DataLayoutConfiguration[
            BlimpVectorHardwareConfiguration, BlimpVectorHitmapDatabaseConfiguration,
            BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
        ]
    ]
):
    def perform_operation(
            self,
            hitmap_index: int,
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP-V hitmap count operation on a provided hitmap index.

        @param hitmap_index: Which hitmap to target for bit counting
        """

        # Ensure we are operating on valid hitmap indices
        if hitmap_index >= self.layout_configuration.database_configuration.hitmap_count:
            raise RuntimeError(f"The hitmap index {hitmap_index} is out of bounds. The current configuration "
                               f"only supports {self.layout_configuration.database_configuration.hitmap_count} hitmaps")

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count

        hitmap_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin()
        count = 0

        # Iterate over all hitmap rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
        )
        for h in range(rows_per_hitmap):
            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; hitmap row calculation",
            )
            # Calculate the hitmap we are targeting
            hitmap_row = hitmap_base + h

            # move hitmap[index] into v1
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_v1,
                row=hitmap_row,
            )

            # count the number of bits set, use pop count if the hardware is present, otherwise use software
            if self.layout_configuration.hardware_configuration.blimpv_extension_vpopcount:
                runtime += self.simulator.blimpv_bit_popcount(
                    register_a=self.simulator.blimp_v1,
                    sew=self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                    stride=self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                )
            else:
                if self.layout_configuration.hardware_configuration.blimp_extension_popcount:
                    result_method = self.simulator.blimp_bit_popcount
                else:
                    result_method = self.simulator.blimp_bit_count
                runtime += result_method(
                    register=self.simulator.blimp_v1,
                    start_index=0,
                    end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                    element_width=self.hardware.hardware_configuration.blimp_processor_bit_architecture // 8,
                )

            # update our count
            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; count update",
            )
            count += byte_array_to_int(
                self.simulator.registers[self.simulator.blimp_v1]
                [0: self.hardware.hardware_configuration.blimp_processor_bit_architecture // 8]
            )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
            )

        runtime += self.simulator.blimp_end()

        # We have finished the query, return a dummy result

        result = HitmapResult([], 0)
        result.result_count = count
        return runtime, result
