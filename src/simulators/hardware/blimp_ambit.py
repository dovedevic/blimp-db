from src.hardware.architectures import BlimpAmbitBank
from src.simulators.hardware import SimulatedBlimpBank, SimulatedAmbitBank
from src.simulators.result import RuntimeResult


class SimulatedBlimpAmbitBank(SimulatedBlimpBank, SimulatedAmbitBank):
    """Defines simulation parameters for a BLIMP-orchestrated AMBIT-capable DRAM Bank"""
    def __init__(
            self,
            bank_hardware: BlimpAmbitBank,
            logger=None
            ):
        super(SimulatedBlimpAmbitBank, self).__init__(bank_hardware, logger)
        self.bank_hardware = bank_hardware

        self._logger.info(f"blimp-ambit simulator loaded")

    def blimp_ambit_dispatch(self, return_labels=True) -> RuntimeResult:
        """Have the CPU send an AMBIT command sequence"""
        return self.blimp_cycle(
            cycles=1, 
            label='bbop[ambit]  ; blimp dispatch' if return_labels else ""
        )
