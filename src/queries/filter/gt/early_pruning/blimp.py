from src.queries.filter._generic.early_pruning.blimp import _BlimpHitmapEarlyPruningGenericScalarALO
from src.queries.filter._generic.operations import GenericArithmeticLogicalOperation
from src.simulators.result import RuntimeResult, HitmapResult


class BlimpHitmapEarlyPruningGreaterThan(_BlimpHitmapEarlyPruningGenericScalarALO):
    def perform_operation(
            self,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0,
            **kwargs
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP GREATER THAN (>) query operation. If the PI/Key field is segmented, specify the segment offset
        and its size, as well as the value to check against. The value must be less than the maximum size expressed by
        the provided size. Return debug labels if specified.

        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        return self._perform_operation(
            pi_element_size_bytes=pi_element_size_bytes,
            value=value,
            operation=GenericArithmeticLogicalOperation.GT,
            return_labels=return_labels,
            hitmap_index=hitmap_index
        )
