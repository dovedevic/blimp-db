from src.queries.query import Query
from src.simulators.result import RuntimeResult, SimulationResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.configurations.hardware.blimp import BlimpHardwareConfiguration
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures.blimp import BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
from src.utils.bitmanip import byte_array_to_int

from src.simulators.blimp import SimulatedBlimpBank


class _BlimpHitmapCount(
    Query[
        SimulatedBlimpBank,
        DataLayoutConfiguration[
            BlimpHardwareConfiguration, BlimpHitmapDatabaseConfiguration,
            BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
        ]
    ]
):
    def _perform_operation(
            self,
            hitmap_index: int,
            return_labels: bool=False,
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a BLIMP hitmap count operation on a provided hitmap index.

        @param hitmap_index: Which hitmap to target for bit counting
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

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin(return_labels)
        count = 0

        # Iterate over all hitmap rows
        runtime += self.simulator.blimp_cycle(3, "; loop start", return_labels)
        for h in range(rows_per_hitmap):
            runtime += self.simulator.blimp_cycle(1, "; hitmap row calculation", return_labels)
            # Calculate the hitmap we are targeting
            hitmap_row = hitmap_base + h

            # move hitmap[index] into v1
            runtime += self.simulator.blimp_load_register(self.simulator.blimp_v1, hitmap_row, return_labels)

            # count the number of bits set
            runtime += self.simulator.blimp_bit_count(
                self.simulator.blimp_v1,
                0,
                self.hardware.hardware_configuration.row_buffer_size_bytes,
                self.hardware.hardware_configuration.blimp_processor_bit_architecture // 8,
                return_labels
            )

            # update our count
            runtime += self.simulator.blimp_cycle(1, "; count update", return_labels)
            count += byte_array_to_int(
                self.simulator.registers[self.simulator.blimp_v1]
                [0: self.hardware.hardware_configuration.blimp_processor_bit_architecture // 8]
            )

            runtime += self.simulator.blimp_cycle(2, "; loop return", return_labels)
        runtime += self.simulator.blimp_end(return_labels)

        # We have finished the query, return a dummy result

        result = SimulationResult([])
        result.result_count = count
        return runtime, result


class BlimpHitmapCount(_BlimpHitmapCount):
    def perform_operation(
            self,
            hitmap_index: int,
            return_labels: bool=False
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a BLIMP hitmap count operation on a provided hitmap index.

        @param hitmap_index: Which hitmap to target for bit counting
        @param return_labels: Whether to return debug labels with the RuntimeResult history

        """
        return self._perform_operation(
            hitmap_index=hitmap_index,
            return_labels=return_labels,
        )
