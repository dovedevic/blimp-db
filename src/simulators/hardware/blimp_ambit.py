from src.hardware.architectures import BlimpAmbitBank
from src.simulators.hardware import SimulatedBlimpBank, SimulatedAmbitBank
from src.simulators.result import RuntimeResult


class SimulatedBlimpAmbitBank(SimulatedBlimpBank, SimulatedAmbitBank):
    """Defines simulation parameters for a BLIMP-orchestrated AMBIT-capable DRAM Bank"""
    def __init__(
            self,
            bank_hardware: BlimpAmbitBank,
            logger=None,
            runtime_class=RuntimeResult
            ):
        super(SimulatedBlimpAmbitBank, self).__init__(bank_hardware, logger, runtime_class)

        self._logger.info(f"blimp-ambit simulator loaded")

    def blimp_ambit_dispatch(self, **runtime_kwargs) -> RuntimeResult:
        """Have the CPU send an AMBIT command sequence"""
        if 'label' not in runtime_kwargs:
            runtime_kwargs['label'] = 'bbop[ambit]  ; blimp dispatch'
        return self.blimp_cycle(
            cycles=1,
            **runtime_kwargs
        )
