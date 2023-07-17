from src.queries.filter_emit_index._generic.blimpv import _BlimpVFilterEmitIndexGenericScalarALO
from src.queries.filter_emit_index._generic.operations import GenericArithmeticLogicalOperation
from src.simulators.result import RuntimeResult, HitmapResult


class BlimpVLessThanEmitIndex(_BlimpVFilterEmitIndexGenericScalarALO):
    def perform_operation(
            self,
            pi_element_size_bytes: int,
            value: int,
            output_array_start_row: int,
            output_index_size_bytes: int,
            **kwargs
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP-V LESS THAN (<) query operation. If the PI/Key field is segmented, specify the segment offset
        and its size, as well as the value to check against. The value must be less than the maximum size expressed by
        the provided size. Return debug labels if specified.

        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param output_array_start_row: The row number where the output array begins
        @param output_index_size_bytes: The number of bytes to use for index hit values in the output array
        """
        return self._perform_operation(
            pi_element_size_bytes=pi_element_size_bytes,
            value=value,
            operation=GenericArithmeticLogicalOperation.LT,
            output_array_start_row=output_array_start_row,
            output_index_size_bytes=output_index_size_bytes,
        )
