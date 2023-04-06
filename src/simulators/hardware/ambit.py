from src.hardware.architectures import AmbitBank
from src.simulators.hardware import SimulatedBank
from src.simulators.result import RuntimeResult


class SimulatedAmbitBank(SimulatedBank[AmbitBank]):
    """Defines simulation parameters for an AMBIT-capable DRAM Bank"""
    def __init__(
            self,
            bank_hardware: AmbitBank,
            logger=None
            ):
        super(SimulatedAmbitBank, self).__init__(bank_hardware, logger)
        self.bank_hardware = bank_hardware

        # Define ambit common-used values
        self._ambit_one = (2**(self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8)) - 1
        self._ambit_zero = 0

        # C/T Mappings
        self._ambit_t_map = {}
        self._ambit_t_base = self.bank_hardware.hardware_configuration.bank_rows - \
            self.bank_hardware.hardware_configuration.ambit_compute_register_rows
        for t in range(self.bank_hardware.hardware_configuration.ambit_compute_register_rows):
            self._ambit_t_map[self._ambit_t_base + t] = t

        # DCC Mappings
        self._ambit_dcc_map = {}
        self._ambit_dcc_base = self._ambit_t_base - (2 * self.bank_hardware.hardware_configuration.ambit_dcc_rows)
        for dcc in range(self.bank_hardware.hardware_configuration.ambit_dcc_rows):
            # DCCi / !DCCi
            self._ambit_dcc_map[self._ambit_dcc_base + 2 * dcc] = (self._ambit_dcc_base + 2 * dcc + 1, dcc)
            self._ambit_dcc_map[self._ambit_dcc_base + 2 * dcc + 1] = (self._ambit_dcc_base + 2 * dcc, dcc)

        # Define static row mappings
        self._ambit_control_0_row = self._ambit_dcc_base - 2
        self._ambit_control_1_row = self._ambit_dcc_base - 1

        self._logger.info(f"ambit simulator loaded")

    def reset_ambit_control_rows(self):
        """Reset/Initialize all ambit controlled rows; This sets the C-group, and defines the B-group rows"""
        self._logger.info("initializing ambit-reserved control and bitwise rows, temporary ambit D-group space")

        # Set the C-group rows
        # C0
        self.bank_hardware.set_raw_row(self._ambit_control_0_row, self._ambit_zero)
        # C1
        self.bank_hardware.set_raw_row(self._ambit_control_1_row, self._ambit_one)

        # Set the B-group rows
        # Set the Dual-Contact-Cells (DCC)
        for dcc in range(self.bank_hardware.hardware_configuration.ambit_dcc_rows):
            # DCCi / !DCCi
            self.bank_hardware.set_raw_row(self._ambit_dcc_base + 2 * dcc, self._ambit_zero)
            self.bank_hardware.set_raw_row(self._ambit_dcc_base + 2 * dcc + 1, self._ambit_one)
        # Set the Compute/Temp (T) registers
        for t in range(self.bank_hardware.hardware_configuration.ambit_compute_register_rows):
            self.bank_hardware.set_raw_row(self._ambit_t_base + t, self._ambit_zero)

    @property
    def ambit_control_zero_row(self):
        """The Ambit C-group row for all zeros"""
        return self._ambit_control_0_row

    @property
    def ambit_control_one_row(self):
        """The Ambit C-group row for all ones"""
        return self._ambit_control_1_row

    def ambit_compute_register(self, register_index):
        """Get an Ambit B-group temporary register"""
        if register_index >= self.bank_hardware.hardware_configuration.ambit_compute_register_rows:
            raise RuntimeError(f"register T{register_index} does not exist in this configuration")
        return self._ambit_t_base + register_index

    @property
    def ambit_c0(self):
        """Ambit C-group C0"""
        return self.ambit_control_zero_row

    @property
    def ambit_c1(self):
        """Ambit C-group C1"""
        return self.ambit_control_one_row

    @property
    def ambit_t0(self):
        """Ambit B-group T0 register"""
        return self.ambit_compute_register(0)

    @property
    def ambit_t1(self):
        """Ambit B-group T1 register"""
        return self.ambit_compute_register(1)

    @property
    def ambit_t2(self):
        """Ambit B-group T2 register"""
        return self.ambit_compute_register(2)

    @property
    def ambit_t3(self):
        """Ambit B-group T3 register"""
        return self.ambit_compute_register(3)

    @property
    def ambit_t4(self):
        """Ambit B-group T4 register"""
        return self.ambit_compute_register(4)

    @property
    def ambit_t5(self):
        """Ambit B-group T5 register"""
        return self.ambit_compute_register(5)

    @property
    def ambit_t6(self):
        """Ambit B-group T6 register"""
        return self.ambit_compute_register(6)

    def ambit_dcc_register(self, dcc_index):
        """Get an Ambit B-group DCC register"""
        if dcc_index >= self.bank_hardware.hardware_configuration.ambit_dcc_rows:
            raise RuntimeError(f"dcc register DCC{dcc_index} does not exist in this configuration")
        return self._ambit_dcc_base + (dcc_index * 2)

    @property
    def ambit_dcc0(self):
        """Ambit B-group DCC0 register"""
        return self.ambit_dcc_register(0)

    @property
    def ambit_ndcc0(self):
        """Ambit B-group !DCC0 register"""
        return self.ambit_dcc_register(0) + 1

    @property
    def ambit_dcc1(self):
        """Ambit B-group DCC1 register"""
        return self.ambit_dcc_register(1)

    @property
    def ambit_ndcc1(self):
        """Ambit B-group !DCC1 register"""
        return self.ambit_dcc_register(1) + 1

    @property
    def ambit_dcc2(self):
        """Ambit B-group DCC2 register"""
        return self.ambit_dcc_register(2)

    @property
    def ambit_ndcc2(self):
        """Ambit B-group !DCC2 register"""
        return self.ambit_dcc_register(2) + 1

    def _get_row_nice_name(self, row: int):
        if row in self._ambit_dcc_map:
            row_nice_name = f"{'~' if row > self._ambit_dcc_map[row][0] else ''}DCC{self._ambit_dcc_map[row][1]}"
        elif row in self._ambit_t_map:
            row_nice_name = f"T{self._ambit_t_map[row]}"
        elif row == self.ambit_control_zero_row:
            row_nice_name = "C0"
        elif row == self.ambit_control_one_row:
            row_nice_name = "C1"
        else:
            row_nice_name = f"mem[{row}]"
        return row_nice_name

    def ambit_copy(self, src_row: int, dst_row: int, return_labels=True) -> RuntimeResult:
        """Perform an AAP sequence from the src to dst row"""
        # Ensure we are not overwriting C-group rows
        if dst_row == self.ambit_control_one_row or dst_row == self.ambit_control_zero_row:
            raise RuntimeError("cannot write to ambit control rows")
        # Ensure we are not copying across a tethered set of rows
        if src_row in self._ambit_dcc_map and self._ambit_dcc_map[src_row][0] == dst_row:
            raise RuntimeError("cannot read and write to a DCC row pair")

        # Perform the AAP sequence
        self.bank_hardware.copy_row(src_row, dst_row)

        # If the destination is one of the DCC rows, invert the other one
        if dst_row in self._ambit_dcc_map:
            inverted_value = self.bank_hardware.get_inverted_raw_row(dst_row)
            self.bank_hardware.set_raw_row(self._ambit_dcc_map[dst_row][0], inverted_value)

        # Return the result of the operation
        return RuntimeResult(
            self.bank_hardware.hardware_configuration.time_for_AAP_rowclone_ns,
            f"AAP {self._get_row_nice_name(src_row)} -> {self._get_row_nice_name(dst_row)}" if return_labels else ""
        )

    def ambit_invert(self, src_row: int, dcc_row: int, dst_row: int, return_labels=True) -> RuntimeResult:
        """Perform a row inversion from src to dst row using AAP with a specified DCC row"""
        # Ensure the DCC row specified is a DCC row
        if dcc_row not in self._ambit_dcc_map:
            raise RuntimeError(f"dcc_row {dcc_row} is not a DCC row in this configuration")

        # Copy data from src to one half of the DCC
        result_from = self.ambit_copy(src_row, dcc_row, return_labels)
        # Use the other tethered part of the DCC for the inversion, and copy it to the dst
        result_to = self.ambit_copy(self._ambit_dcc_map[dcc_row][0], dst_row, return_labels)

        # Return the runtime results of these two AAPs
        return result_from + result_to

    def ambit_tra(self, a_row: int, b_row: int, c_row: int, return_labels=True) -> RuntimeResult:
        """Perform a TRA sequence on rows A, B, and C"""
        # Ensure a-b, b-c, and a-c are not DCC paired rows
        if a_row in self._ambit_dcc_map and self._ambit_dcc_map[a_row][0] == b_row:
            raise RuntimeError("cannot perform TRA when 'a' and 'b' operands are DCC pairs")
        if b_row in self._ambit_dcc_map and self._ambit_dcc_map[b_row][0] == c_row:
            raise RuntimeError("cannot perform TRA when 'b' and 'c' operands are DCC pairs")
        if c_row in self._ambit_dcc_map and self._ambit_dcc_map[c_row][0] == a_row:
            raise RuntimeError("cannot perform TRA when 'c' and 'a' operands are DCC pairs")

        # Ensure a, b, c are not C-group (ambit control) rows
        if a_row == self.ambit_control_one_row or a_row == self.ambit_control_zero_row:
            raise RuntimeError("cannot perform TRA when operand 'a' is a read-only ambit control row")
        if b_row == self.ambit_control_one_row or b_row == self.ambit_control_zero_row:
            raise RuntimeError("cannot perform TRA when operand 'b' is a read-only ambit control row")
        if c_row == self.ambit_control_one_row or c_row == self.ambit_control_zero_row:
            raise RuntimeError("cannot perform TRA when operand 'c' is a read-only ambit control row")

        # Ensure a, b, c are not D-group (data) rows
        if a_row not in self._ambit_t_map and a_row not in self._ambit_dcc_map:
            raise RuntimeError("cannot perform TRA when operand 'a' is a D-group row")
        if b_row not in self._ambit_t_map and b_row not in self._ambit_dcc_map:
            raise RuntimeError("cannot perform TRA when operand 'b' is a D-group row")
        if c_row not in self._ambit_t_map and c_row not in self._ambit_dcc_map:
            raise RuntimeError("cannot perform TRA when operand 'c' is a D-group row")

        # Perform the TRA
        self.bank_hardware.tra_rows(a_row, b_row, c_row)

        # If one of the operands was a DCC row, update its inverse
        if a_row in self._ambit_dcc_map:
            inverted_value = self.bank_hardware.get_inverted_raw_row(a_row)
            self.bank_hardware.set_raw_row(self._ambit_dcc_map[a_row][0], inverted_value)
        if b_row in self._ambit_dcc_map:
            inverted_value = self.bank_hardware.get_inverted_raw_row(b_row)
            self.bank_hardware.set_raw_row(self._ambit_dcc_map[b_row][0], inverted_value)
        if c_row in self._ambit_dcc_map:
            inverted_value = self.bank_hardware.get_inverted_raw_row(c_row)
            self.bank_hardware.set_raw_row(self._ambit_dcc_map[c_row][0], inverted_value)

        return RuntimeResult(
            self.bank_hardware.hardware_configuration.time_for_TRA_MAJ_ns,
            f"TRA {self._get_row_nice_name(a_row)} {self._get_row_nice_name(b_row)} {self._get_row_nice_name(c_row)}"
            if return_labels else ""
        )

    def ambit_and(self, a_row: int, b_row: int, control_dst: int, return_labels=True) -> RuntimeResult:
        """Perform a bitwise ambit AND operation on B-group A, B using the control_dst as a control register"""
        # Initialize the control register to 0 to initialize ambit AND
        ambit_setup = self.ambit_copy(self._ambit_control_0_row, control_dst, return_labels)

        # Perform the TRA result
        tra_runtime = self.ambit_tra(a_row, b_row, control_dst)

        # Return the result
        return ambit_setup + tra_runtime

    def ambit_or(self, a_row: int, b_row: int, control_dst: int, return_labels=True) -> RuntimeResult:
        """Perform a bitwise ambit OR operation on B-group A, B using the control_dst as a control register"""
        # Initialize the control register to 1 to initialize ambit OR
        ambit_setup = self.ambit_copy(self._ambit_control_1_row, control_dst, return_labels)

        # Perform the TRA result
        tra_runtime = self.ambit_tra(a_row, b_row, control_dst)

        # Return the result
        return ambit_setup + tra_runtime

    def cpu_ambit_dispatch(self, return_labels=True) -> RuntimeResult:
        """Have the CPU send an AMBIT command sequence"""
        return RuntimeResult(
            self.bank_hardware.hardware_configuration.time_to_bank_communicate_ns,
            'bbop[ambit]  ; cpu dispatch' if return_labels else ""
        )
