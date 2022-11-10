from src.queries.query import Query
from src.simulators.result import RuntimeResult, SimulationResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.configurations.hardware.ambit import AmbitHardwareConfiguration
from src.configurations.database.ambit import AmbitHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures.ambit import AmbitHitmapLayoutMetadata, AmbitHitmapRowMapping
from src.queries.logical.operations import HitmapLogicalOperation

from src.simulators.ambit import SimulatedAmbitBank


class _AmbitHitmapLogical(
    Query[
        SimulatedAmbitBank,
        DataLayoutConfiguration[
            AmbitHardwareConfiguration, AmbitHitmapDatabaseConfiguration,
            AmbitHitmapLayoutMetadata, AmbitHitmapRowMapping
        ]
    ]
):
    def _perform_operation(
            self,
            hitmap_index_a: int,
            operation: HitmapLogicalOperation,
            hitmap_index_b: int,
            hitmap_index_result: int=None,
            return_labels: bool=False,
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform an AMBIT logical operation on the specified hitmaps.

        @param hitmap_index_a: The LHS hitmap for the logical operation
        @param operation: What logical operation to perform on the hitmap
        @param hitmap_index_b: The RHS hitmap for the logical operation
        @param hitmap_index_result: The hitmap result for the logical operation
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        """

        # Ensure we are operating on valid hitmap indices
        if hitmap_index_a >= self.layout_configuration.database_configuration.hitmap_count:
            raise RuntimeError(f"The LHS hitmap index {hitmap_index_a} is out of bounds. The current configuration "
                               f"only supports {self.layout_configuration.database_configuration.hitmap_count} hitmaps")
        if hitmap_index_b >= self.layout_configuration.database_configuration.hitmap_count:
            raise RuntimeError(f"The RHS hitmap index {hitmap_index_b} is out of bounds. The current configuration "
                               f"only supports {self.layout_configuration.database_configuration.hitmap_count} hitmaps")
        if hitmap_index_result >= self.layout_configuration.database_configuration.hitmap_count:
            raise RuntimeError(f"The result hitmap index {hitmap_index_result} is out of bounds. The current configuration "
                               f"only supports {self.layout_configuration.database_configuration.hitmap_count} hitmaps")

        # Ensure we have a fresh set of ambit control rows
        self.layout_configuration.reset_ambit_control_rows(self.hardware)

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count

        hitmap_a_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index_a
        hitmap_b_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index_b
        hitmap_r_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index_result

        # Begin
        runtime = self.simulator.cpu_cycle(1, "; prog start", return_labels)  # Just send a dummy command

        # Iterate over all hitmap rows
        runtime += self.simulator.cpu_cycle(3, "; loop start", return_labels)
        for h in range(rows_per_hitmap):
            runtime += self.simulator.cpu_cycle(2, "; hitmap row calculation", return_labels)
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row_a = hitmap_a_base + h
            hitmap_row_b = hitmap_b_base + h
            hitmap_row_r = hitmap_r_base + h

            # Performing the Operation
            # move hitmap[a] into ambit compute region
            runtime += self.simulator.cpu_ambit_dispatch(return_labels)
            runtime += self.simulator.ambit_copy(
                hitmap_row_a,
                self.simulator.ambit_t1,
                return_labels
            )

            # move hitmap[b] into ambit compute region
            runtime += self.simulator.cpu_ambit_dispatch(return_labels)
            runtime += self.simulator.ambit_copy(
                hitmap_row_b,
                self.simulator.ambit_t2,
                return_labels
            )

            # perform hitmap[a] OPERATION hitmap[b]
            runtime += self.simulator.cpu_ambit_dispatch(return_labels)
            if operation == HitmapLogicalOperation.AND:
                runtime += self.simulator.ambit_and(
                    self.simulator.ambit_t1,
                    self.simulator.ambit_t2,
                    self.simulator.ambit_t0,
                    return_labels
                )
            elif operation == HitmapLogicalOperation.OR:
                runtime += self.simulator.ambit_or(
                    self.simulator.ambit_t1,
                    self.simulator.ambit_t2,
                    self.simulator.ambit_t0,
                    return_labels
                )

            # move result into hitmap[r] compute region
            runtime += self.simulator.cpu_ambit_dispatch(return_labels)
            runtime += self.simulator.ambit_copy(
                self.simulator.ambit_t0,
                hitmap_row_r,
                return_labels
            )

            runtime += self.simulator.cpu_cycle(2, "; loop return", return_labels)

        # We have finished the query, fetch the hitmap to one single hitmap row
        hitmap_byte_array = []
        for h in range(rows_per_hitmap):
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index_result + h

            # Append the byte array for the next hitmap sub row
            hitmap_byte_array += self.simulator.bank_hardware.get_row_bytes(hitmap_row)

        result = SimulationResult.from_hitmap_byte_array(
            hitmap_byte_array,
            self.layout_configuration.layout_metadata.total_records_processable
        )
        return runtime, result


class AmbitHitmapLogicalAnd(_AmbitHitmapLogical):
    def perform_operation(
            self,
            hitmap_index_a: int,
            hitmap_index_b: int,
            return_labels: bool=False
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform an AMBIT AND logical operation between hitmap index A and B.

        @param hitmap_index_a: Which hitmap to target for LHS AND
        @param hitmap_index_b: Which hitmap to target for RHS AND
        @param return_labels: Whether to return debug labels with the RuntimeResult history

        """
        return self._perform_operation(
            hitmap_index_a=hitmap_index_a,
            operation=HitmapLogicalOperation.AND,
            hitmap_index_b=hitmap_index_b,
            hitmap_index_result=hitmap_index_a,
            return_labels=return_labels,
        )


class AmbitHitmapLogicalOr(_AmbitHitmapLogical):
    def perform_operation(
            self,
            hitmap_index_a: int,
            hitmap_index_b: int,
            return_labels: bool = False
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform an AMBIT OR logical operation between hitmap index A and B.

        @param hitmap_index_a: Which hitmap to target for LHS OR
        @param hitmap_index_b: Which hitmap to target for RHS OR
        @param return_labels: Whether to return debug labels with the RuntimeResult history

        """
        return self._perform_operation(
            hitmap_index_a=hitmap_index_a,
            operation=HitmapLogicalOperation.OR,
            hitmap_index_b=hitmap_index_b,
            hitmap_index_result=hitmap_index_a,
            return_labels=return_labels,
        )
