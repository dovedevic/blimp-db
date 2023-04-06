from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.configurations.hardware.ambit import BlimpPlusAmbitHardwareConfiguration
from src.configurations.database.ambit import BlimpPlusAmbitHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures.blimp_ambit import BlimpAmbitHitmapLayoutMetadata, BlimpAmbitHitmapRowMapping
from src.queries.logical.operations import HitmapLogicalOperation
from src.simulators.hardware import SimulatedBlimpAmbitBank


class _BlimpAmbitHitmapLogical(
    Query[
        SimulatedBlimpAmbitBank,
        DataLayoutConfiguration[
            BlimpPlusAmbitHardwareConfiguration, BlimpPlusAmbitHitmapDatabaseConfiguration,
            BlimpAmbitHitmapLayoutMetadata, BlimpAmbitHitmapRowMapping
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
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP+AMBIT logical operation on the specified hitmaps.

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

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin(return_labels=return_labels)

        # Iterate over all hitmap rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
            return_labels=return_labels
        )
        for h in range(rows_per_hitmap):
            runtime += self.simulator.blimp_cycle(
                cycles=3,
                label="; hitmap row calculation",
                return_labels=return_labels
            )
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row_a = hitmap_a_base + h
            hitmap_row_b = hitmap_b_base + h
            hitmap_row_r = hitmap_r_base + h

            # Performing the Operation
            # move hitmap[a] into ambit compute region
            runtime += self.simulator.blimp_ambit_dispatch(return_labels=return_labels)
            runtime += self.simulator.ambit_copy(
                src_row=hitmap_row_a,
                dst_row=self.simulator.ambit_t1,
                return_labels=return_labels
            )

            # move hitmap[b] into ambit compute region
            runtime += self.simulator.blimp_ambit_dispatch(return_labels=return_labels)
            runtime += self.simulator.ambit_copy(
                src_row=hitmap_row_b,
                dst_row=self.simulator.ambit_t2,
                return_labels=return_labels
            )

            # perform hitmap[a] OPERATION hitmap[b]
            runtime += self.simulator.blimp_ambit_dispatch(return_labels=return_labels)
            if operation == HitmapLogicalOperation.AND:
                runtime += self.simulator.ambit_and(
                    a_row=self.simulator.ambit_t1,
                    b_row=self.simulator.ambit_t2,
                    control_dst=self.simulator.ambit_t0,
                    return_labels=return_labels
                )
            elif operation == HitmapLogicalOperation.OR:
                runtime += self.simulator.ambit_or(
                    a_row=self.simulator.ambit_t1,
                    b_row=self.simulator.ambit_t2,
                    control_dst=self.simulator.ambit_t0,
                    return_labels=return_labels
                )

            # move result into hitmap[r] compute region
            runtime += self.simulator.blimp_ambit_dispatch(return_labels=return_labels)
            runtime += self.simulator.ambit_copy(
                src_row=self.simulator.ambit_t0,
                dst_row=hitmap_row_r,
                return_labels=return_labels
            )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
                return_labels=return_labels
            )

        runtime += self.simulator.blimp_end(return_labels=return_labels)

        # We have finished the query, fetch the hitmap to one single hitmap row
        hitmap_byte_array = []
        for h in range(rows_per_hitmap):
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index_result + h

            # Append the byte array for the next hitmap sub row
            hitmap_byte_array += self.simulator.bank_hardware.get_row_bytes(hitmap_row)

        result = HitmapResult.from_hitmap_byte_array(
            hitmap_byte_array,
            self.layout_configuration.layout_metadata.total_records_processable
        )
        return runtime, result


class BlimpAmbitHitmapLogicalAnd(_BlimpAmbitHitmapLogical):
    def perform_operation(
            self,
            hitmap_index_a: int,
            hitmap_index_b: int,
            return_labels: bool=False
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP+AMBIT AND logical operation between hitmap index A and B.

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


class BlimpAmbitHitmapLogicalOr(_BlimpAmbitHitmapLogical):
    def perform_operation(
            self,
            hitmap_index_a: int,
            hitmap_index_b: int,
            return_labels: bool = False
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP+AMBIT OR logical operation between hitmap index A and B.

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
