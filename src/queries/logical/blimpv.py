from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration
from src.configurations.database.blimp import BlimpVectorHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures.blimp import BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
from src.queries.logical.operations import HitmapLogicalOperation
from src.simulators.hardware import SimulatedBlimpVBank


class _BlimpVHitmapLogical(
    Query[
        SimulatedBlimpVBank,
        DataLayoutConfiguration[
            BlimpVectorHardwareConfiguration, BlimpVectorHitmapDatabaseConfiguration,
            BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
        ]
    ]
):
    def _perform_operation(
            self,
            hitmap_index_a: int,
            operation: HitmapLogicalOperation,
            hitmap_index_b: int,
            hitmap_index_result: int=None,
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP-V logical operation on the specified hitmaps.

        @param hitmap_index_a: The LHS hitmap for the logical operation
        @param operation: What logical operation to perform on the hitmap
        @param hitmap_index_b: The RHS hitmap for the logical operation
        @param hitmap_index_result: The hitmap result for the logical operation
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

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count

        hitmap_a_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index_a
        hitmap_b_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index_b
        hitmap_r_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index_result

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin()

        # Iterate over all hitmap rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
        )
        for h in range(rows_per_hitmap):
            runtime += self.simulator.blimp_cycle(
                cycles=3,
                label="; hitmap row calculation",
            )
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row_a = hitmap_a_base + h
            hitmap_row_b = hitmap_b_base + h
            hitmap_row_r = hitmap_r_base + h

            # Performing the Operation
            # move hitmap[a] into v1
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_v1,
                row=hitmap_row_a,
            )

            # move hitmap[b] into v2
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_v2,
                row=hitmap_row_b,
            )

            # perform hitmap[a] OPERATION hitmap[b]
            if operation == HitmapLogicalOperation.AND:
                runtime += self.simulator.blimpv_alu_int_and(
                    register_a=self.simulator.blimp_v1,
                    register_b=self.simulator.blimp_v2,
                    sew=self.hardware.hardware_configuration.blimpv_sew_max_bytes,
                    stride=self.hardware.hardware_configuration.blimpv_sew_max_bytes,
                )
            elif operation == HitmapLogicalOperation.OR:
                runtime += self.simulator.blimpv_alu_int_or(
                    register_a=self.simulator.blimp_v1,
                    register_b=self.simulator.blimp_v2,
                    sew=self.hardware.hardware_configuration.blimpv_sew_max_bytes,
                    stride=self.hardware.hardware_configuration.blimpv_sew_max_bytes,
                )

            # move result into hitmap[r] compute region
            runtime += self.simulator.blimp_save_register(
                register=self.simulator.blimp_v2,
                row=hitmap_row_r,
            )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
            )

        runtime += self.simulator.blimp_end()

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


class BlimpVHitmapLogicalAnd(_BlimpVHitmapLogical):
    def perform_operation(
            self,
            hitmap_index_a: int,
            hitmap_index_b: int,
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP-V AND logical operation between hitmap index A and B.

        @param hitmap_index_a: Which hitmap to target for LHS AND
        @param hitmap_index_b: Which hitmap to target for RHS AND
        """
        return self._perform_operation(
            hitmap_index_a=hitmap_index_a,
            operation=HitmapLogicalOperation.AND,
            hitmap_index_b=hitmap_index_b,
            hitmap_index_result=hitmap_index_a,
        )


class BlimpVHitmapLogicalOr(_BlimpVHitmapLogical):
    def perform_operation(
            self,
            hitmap_index_a: int,
            hitmap_index_b: int,
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP-V OR logical operation between hitmap index A and B.

        @param hitmap_index_a: Which hitmap to target for LHS OR
        @param hitmap_index_b: Which hitmap to target for RHS OR
        """
        return self._perform_operation(
            hitmap_index_a=hitmap_index_a,
            operation=HitmapLogicalOperation.OR,
            hitmap_index_b=hitmap_index_b,
            hitmap_index_result=hitmap_index_a,
        )
