import math

from src.hardware.architectures import BlimpBank, BlimpVectorBank
from src.simulators.simulator import SimulatedBank
from src.simulators.result import RuntimeResult
from src.utils.bitmanip import byte_array_to_int, int_to_byte_array


class SimulatedBlimpBank(
    SimulatedBank[BlimpBank]
):
    """Defines simulation parameters for a BLIMP-capable DRAM Bank"""
    def __init__(
            self,
            bank_hardware: BlimpBank,
            logger=None
            ):
        super(SimulatedBlimpBank, self).__init__(bank_hardware, logger)
        self.bank_hardware = bank_hardware

        self.registers = {
            self._v0(): [],
            self._instr_buffer(): [],
            self._data_pad(): [],
            self._v1(): [],
            self._v2(): [],
        }

        self._logger.info(f"blimp simulator loaded")

    @staticmethod
    def _v0():
        """Register name for the pseudo-register linked to the row buffer"""
        return "v0"

    @staticmethod
    def _v1():
        """Register name for the first BLIMP-V vector register"""
        return "v1"

    @staticmethod
    def _v2():
        """Register name for the second BLIMP-V vector register"""
        return "v2"

    @staticmethod
    def _instr_buffer():
        """Register name for the Instruction Buffer"""
        return "instr_buffer"

    @staticmethod
    def _data_pad():
        """Register name for the Data Pad"""
        return "data_pad"

    @property
    def blimp_v0(self):
        """BLIMP V0 pseudo-row-buffer register"""
        return self._v0()

    @property
    def blimp_v1(self):
        """BLIMP-V V1 vector register"""
        return self._v1()

    @property
    def blimp_v2(self):
        """BLIMP-V V2 vector register"""
        return self._v2()

    @property
    def blimp_instruction_buffer(self):
        """BLIMP Instruction Buffer register"""
        return self._instr_buffer()

    @property
    def blimp_data_scratchpad(self):
        """BLIMP Data Scratchpad register"""
        return self._data_pad()

    def blimp_cycle(self, cycles=1, label="", return_labels=True) -> RuntimeResult:
        """Perform a specified number of BLIMP cycles"""
        if cycles <= 0:
            raise ValueError("argument 'cycles' cannot be less than one")
        runtime = RuntimeResult(
            self.bank_hardware.hardware_configuration.time_per_blimp_cycle_ns,
            label if return_labels else ""
        )
        for c in range(cycles - 1):
            runtime.step(self.bank_hardware.hardware_configuration.time_per_blimp_cycle_ns)
        return runtime

    def blimp_begin(self, return_labels=True) -> RuntimeResult:
        """Set the BLIMP-enable signal high to begin BLIMP bank operation"""

        # Initialize the BLIMP instruction buffer register to the first row, others are garbage (but loaded here)
        for r in self.registers:
            self.blimp_load_register(r, 0)

        return RuntimeResult(
            self.bank_hardware.hardware_configuration.time_to_bank_communicate_ns,
            "BLIMP ENABLE" if return_labels else ""
        ) + RuntimeResult(
            self.bank_hardware.hardware_configuration.time_to_row_activate_ns +
            self.bank_hardware.hardware_configuration.time_to_precharge_ns,
            "BLIMP CODE FETCH" if return_labels else ""
        ) + self.blimp_cycle(
            cycles=5, label="; setup", return_labels=return_labels
        )

    def blimp_end(self, return_labels=True) -> RuntimeResult:
        """Set the BLIMP-enable signal low to complete BLIMP bank operation"""
        # All we are simulating is a row access and read to the BLIMP transfer mux
        return RuntimeResult(
            self.bank_hardware.hardware_configuration.time_to_bank_communicate_ns,
            "BLIMP DISABLE" if return_labels else ""
        )

    def _ensure_register_exists(self, *registers):
        for register in registers:
            assert register in self.registers, f"Register '{register}' does not exist"

    def _ensure_writable_register(self, *registers):
        self._ensure_register_exists(*registers)
        for register in registers:
            assert register != self.blimp_v0, f"Cannot transfer into register V0"

    def _ensure_valid_nary_operation(self, start_index, end_index, element_width, stride, *registers):
        self._ensure_writable_register(*registers)
        word_size = self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8
        assert element_width > 0, "Element widths must be positive"
        assert end_index >= 0 and start_index >= 0, "Byte indices must be positive"
        assert end_index <= self.bank_hardware.hardware_configuration.row_buffer_size_bytes, "End byte indices must be smaller than the row buffer size"
        assert end_index >= start_index, "end_index must come after start_index"
        assert end_index - start_index <= self.bank_hardware.hardware_configuration.row_buffer_size_bytes, "Byte index range exceeds hardware capability"
        assert element_width <= word_size, "Element width must be smaller than this processors word size"
        assert word_size % element_width == 0, "Element width must be a multiple of the word size"
        assert stride % element_width == 0, f"Stride of {stride} must be a direct multiple of the element size {element_width}"

    def blimp_load_register(self, register, row: int, return_labels=True) -> RuntimeResult:
        """Fetch data into a specified internal register, via v0"""
        # Sanity checking
        self._ensure_register_exists(register)

        result = self.blimp_cycle(return_labels=return_labels)

        # Fetch the row via the row buffer
        self.registers[self.blimp_v0] = self.bank_hardware.get_row_bytes(row)
        result += RuntimeResult(
            self.bank_hardware.hardware_configuration.time_to_row_activate_ns +
            self.bank_hardware.hardware_configuration.time_to_precharge_ns,
            f"mem[{hex(row * self.bank_hardware.hardware_configuration.row_buffer_size_bytes)}] -> {self.blimp_v0}"
            if return_labels else ""
        )

        # The row buffer (and now v0) is loaded with data, transfer it via the mux if necessary
        if register != self.blimp_v0:
            self.registers[register] = self.registers[self.blimp_v0]

            # Add time to transfer the row buffer/v0 to the specified mux destination
            result += RuntimeResult(
                self.bank_hardware.hardware_configuration.time_to_v0_transfer_ns,
                f"\t{self.blimp_v0} -> {register}" if return_labels else ""
            )

        # Return the result of the operation
        return result

    def blimp_transfer_register(self, register_a, register_b, return_labels=True) -> RuntimeResult:
        """Transfer data from a specified internal register, to another specified register (not v0)"""
        # Sanity checking
        self._ensure_writable_register(register_a, register_b)

        result = self.blimp_cycle(return_labels=return_labels)

        # Add time to transfer the row buffer/v0 to the specified mux destination
        result += RuntimeResult(
            self.bank_hardware.hardware_configuration.time_to_v0_transfer_ns,
            f"\t{register_a} -> {register_b}" if return_labels else ""
        )

        # Return the result of the operation
        return result

    def blimp_get_register(self, register) -> [int]:
        """Fetch the data for a BLIMP or BLIMP-V register"""
        self._ensure_register_exists(register)
        return self.registers[register]

    def blimp_save_register(self, register, row: int, return_labels=True) -> RuntimeResult:
        """Save a specified internal register into a row, via v0"""
        # Sanity checking
        self._ensure_register_exists(register)

        result = self.blimp_cycle(return_labels=return_labels)

        # Transfer data to v0 only when we aren't already operating on it
        if register != self.blimp_v0:
            # Transfer the register via the mux to v0/row buffer
            self.registers[self.blimp_v0] = self.registers[register]
            result += RuntimeResult(
                self.bank_hardware.hardware_configuration.time_to_v0_transfer_ns,
                f"\t{register} -> {self.blimp_v0}" if return_labels else ""
            )

        # Save v0 into the bank memory
        self.bank_hardware.set_row_bytes(row, self.registers[self.blimp_v0])
        result += RuntimeResult(
            self.bank_hardware.hardware_configuration.time_to_row_activate_ns +
            self.bank_hardware.hardware_configuration.time_to_precharge_ns,
            f"{self.blimp_v0} -> mem[{hex(row * self.bank_hardware.hardware_configuration.row_buffer_size_bytes)}]"
            if return_labels else ""
        )

        # Return the result of the operation
        return result

    def _blimp_set_register_to_(self, ones: bool, register, return_labels=True) -> RuntimeResult:
        """Set the data scratchpad to all zeros or ones"""
        total_bits = self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8
        processor_bits = self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture
        operations = int(math.ceil(total_bits / processor_bits))
        cycles_per_operation = 2  # mov x <- ones | jne i

        result = self.blimp_cycle(label="; loop start", return_labels=return_labels)
        result += self.blimp_cycle(
            cycles=operations * cycles_per_operation
        )

        self.registers[register] = \
            int_to_byte_array(
                (2**(self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8)) - 1 if ones else 0,
                self.bank_hardware.hardware_configuration.row_buffer_size_bytes
            )

        return result

    def blimp_set_scratchpad_to_zero(self, return_labels=True) -> RuntimeResult:
        """Set the data scratchpad to all zeros"""
        return self._blimp_set_register_to_(False, self.blimp_data_scratchpad, return_labels)

    def blimp_set_scratchpad_to_one(self, return_labels=True) -> RuntimeResult:
        """Set the data scratchpad to all ones"""
        return self._blimp_set_register_to_(True, self.blimp_data_scratchpad, return_labels)

    def blimp_set_v1_to_zero(self, return_labels=True) -> RuntimeResult:
        """Set v1 to all zeros"""
        return self._blimp_set_register_to_(False, self.blimp_v1, return_labels)

    def blimp_set_v1_to_one(self, return_labels=True) -> RuntimeResult:
        """Set v1 to all ones"""
        return self._blimp_set_register_to_(True, self.blimp_v1, return_labels)

    def blimp_set_v2_to_zero(self, return_labels=True) -> RuntimeResult:
        """Set v2 to all zeros"""
        return self._blimp_set_register_to_(False, self.blimp_v2, return_labels)

    def blimp_set_v2_to_one(self, return_labels=True) -> RuntimeResult:
        """Set v2 to all ones"""
        return self._blimp_set_register_to_(True, self.blimp_v2, return_labels)

    def blimp_is_register_zero(self, register) -> bool:
        self._ensure_register_exists(register)
        # This can be done for free if our registers have a ZF
        return byte_array_to_int(self.registers[register]) == 0

    def _blimp_alu_unary_operation(self, register_a, start_index, end_index, element_width, stride, operation, invert):
        """Perform a BLIMP unary operation and store the result in Register A"""
        # Sanity checking
        self._ensure_valid_nary_operation(start_index, end_index, element_width, stride, register_a)

        elements = math.ceil((end_index - start_index) // element_width)
        word_size = self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8

        for element in range(elements):
            a = byte_array_to_int(
                self.registers
                [register_a]
                [element * element_width + start_index: element * element_width + start_index + element_width]
            )

            if element % (stride // element_width) == 0:  # is this sew chunk to be operated on due to the stride?
                c = operation(a)
                c = ((~c if invert else c) & (2 ** (word_size * 8)) - 1)  # eliminate any carry's and invert if needed
            else:
                c = a  # just keep what was there
            c = int_to_byte_array(c, element_width)
            for byte in range(element_width):
                self.registers[register_a][element * element_width + start_index + byte] = c[byte]

    def _blimp_alu_binary_operation(self, register_a, register_b, start_index, end_index, element_width, stride,
                                    operation, invert):
        """Perform a BLIMP-V binary operation and store the result in Register B"""
        # Sanity checking
        self._ensure_valid_nary_operation(start_index, end_index, element_width, stride, register_a, register_b)

        word_size = self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8
        elements = math.ceil((end_index - start_index) // element_width)

        for element in range(elements):
            a = byte_array_to_int(
                self.registers
                [register_a]
                [element * element_width + start_index: element * element_width + start_index + element_width]
            )
            b = byte_array_to_int(
                self.registers
                [register_b]
                [element * element_width + start_index: element * element_width + start_index + element_width]
            )

            if element % (stride // element_width) == 0:  # is this sew chunk to be operated on due to the stride?
                c = operation(a, b)
                c = ((~c if invert else c) & (2 ** (word_size * 8)) - 1)  # eliminate any carry's and invert if needed
            else:
                c = a  # just keep what was in the source register
            c = int_to_byte_array(c, word_size)
            for byte in range(word_size):
                self.registers[register_b][element * element_width + start_index + byte] = c[byte]

    def _blimp_alu_int_un_op(self, register_a, start_index, end_index, element_width, stride, operation, invert,
                             op_name, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP unary operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        self._blimp_alu_unary_operation(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            operation,
            invert
        )

        # Calculate the number of cycles this operation takes
        cycles = 1
        # Calculate how many elements we are processing
        elements = math.ceil((end_index - start_index) / element_width)
        # Calculate how many source operands there are
        operands = elements // (stride // element_width)
        # Calculate how many ALU rounds are needed
        alu_rounds = operands
        # Assumption; ALU takes a CPU cycle to execute, at least once extra cycle for looping per alu round
        cycles += alu_rounds * 2

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a}[{start_index}:{end_index}] <- {op_name} {register_a}[{start_index}:{end_index}]",
            return_labels=return_labels
        )

    def _blimp_alu_int_bin_op(self, register_a, register_b, start_index, end_index, element_width, stride, operation,
                              invert, op_name, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP binary operation on register a and b starting at a specified byte index and ending on a
        specified byte index then store the result in register b
        """
        # Perform the operation
        self._blimp_alu_binary_operation(
            register_a,
            register_b,
            start_index,
            end_index,
            element_width,
            stride,
            operation,
            invert
        )

        # Calculate the number of cycles this operation takes
        cycles = 1
        # Calculate how many words we are processing
        elements = math.ceil((end_index - start_index) / element_width)
        # Calculate how many source operands there are
        operands = elements // (stride // element_width)
        # Calculate how many ALU rounds are needed
        alu_rounds = operands
        # Assumption; ALU takes a CPU cycle to execute, at least once extra cycle for looping per alu round
        cycles += alu_rounds * 2

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_b}[{start_index}:{end_index}] <- {register_a}[{start_index}:{end_index}] {op_name} {register_b}[{start_index}:{end_index}]",
            return_labels=return_labels
        )

    def blimp_alu_int_and(self, register_a, register_b, start_index, end_index, element_width, stride,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP AND operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a, b: a & b,
            False,
            "AND",
            return_labels
        )

    def blimp_alu_int_or(self, register_a, register_b, start_index, end_index, element_width, stride, return_labels=True
                         ) -> RuntimeResult:
        """
        Perform a BLIMP OR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a, b: a | b,
            False,
            "OR",
            return_labels
        )

    def blimp_alu_int_xor(self, register_a, register_b, start_index, end_index, element_width, stride,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP XOR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a, b: a ^ b,
            False,
            "XOR",
            return_labels
        )

    def blimp_alu_int_nand(self, register_a, register_b, start_index, end_index, element_width, stride,
                           return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP NAND operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a, b: a & b,
            True,
            "NAND",
            return_labels
        )

    def blimp_alu_int_nor(self, register_a, register_b, start_index, end_index, element_width, stride,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP NOR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a, b: a | b,
            True,
            "NOR",
            return_labels
        )

    def blimp_alu_int_xnor(self, register_a, register_b, start_index, end_index, element_width, stride,
                           return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP XNOR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a, b: a ^ b,
            True,
            "XNOR",
            return_labels
        )

    def blimp_alu_int_add(self, register_a, register_b, start_index, end_index, element_width, stride,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP ADD operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a, b: a + b,
            False,
            "ADD",
            return_labels
        )

    def blimp_alu_int_sub(self, register_a, register_b, start_index, end_index, element_width, stride,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP SUB operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a, b: a - b,
            False,
            "SUB",
            return_labels
        )

    def blimp_alu_int_not(self, register_a, start_index, end_index, element_width, stride, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP NOT operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: ~a,
            False,
            "NOT",
            return_labels
        )

    def blimp_alu_int_acc(self, register_a, start_index, end_index, element_width, stride, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP ACC operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: a + 1,
            False,
            "ACC",
            return_labels
        )

    def blimp_alu_int_zero(self, register_a, start_index, end_index, element_width, stride, return_labels=True
                           ) -> RuntimeResult:
        """
        Perform a BLIMP ISZERO operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: int(a == 0),
            False,
            "ZERO",
            return_labels
        )

    def blimp_alu_int_xnor_val(self, register_a, start_index, end_index, element_width, stride, value,
                               return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP scalar XNOR operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        assert 0 <= value <= 2 ** self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture - 1, \
            "Scalar Operation Value bit-width mismatch"
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: a ^ value,
            True,
            "XNOR",
            return_labels
        )

    def blimp_alu_int_eq_val(self, register_a, start_index, end_index, element_width, stride, value, return_labels=True
                             ) -> RuntimeResult:
        """Perform a BLIMP EQUAL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: int(a == value),
            False,
            "EQ",
            return_labels
        )

    def blimp_alu_int_neq_val(self, register_a, start_index, end_index, element_width, stride, value, return_labels=True
                              ) -> RuntimeResult:
        """Perform a BLIMP NOT EQUAL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: int(a != value),
            False,
            "NEQ",
            return_labels
        )

    def blimp_alu_int_lt_val(self, register_a, start_index, end_index, element_width, stride, value, return_labels=True
                             ) -> RuntimeResult:
        """Perform a BLIMP LESS THAN operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: int(a < value),
            False,
            "LT",
            return_labels
        )

    def blimp_alu_int_gt_val(self, register_a, start_index, end_index, element_width, stride, value, return_labels=True
                             ) -> RuntimeResult:
        """Perform a BLIMP GREATER THAN operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: int(a > value),
            False,
            "GT",
            return_labels
        )

    def blimp_alu_int_lte_val(self, register_a, start_index, end_index, element_width, stride, value, return_labels=True
                              ) -> RuntimeResult:
        """
        Perform a BLIMP LESS THAN OR EQUAL operation on a register on SEW bytes and store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: int(a <= value),
            False,
            "LTE",
            return_labels
        )

    def blimp_alu_int_gte_val(self, register_a, start_index, end_index, element_width, stride, value, return_labels=True
                              ) -> RuntimeResult:
        """
        Perform a BLIMP GREATER THAN OR EQUAL operation on a register on SEW bytes and store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_index,
            end_index,
            element_width,
            stride,
            lambda a: int(a >= value),
            False,
            "GTE",
            return_labels
        )

    def blimp_coalesce_register_hitmap(self, register_a, start_index, end_index, element_width, stride, bit_offset,
                                       return_labels=True) -> RuntimeResult:
        """
        Coalesce a bitmap in register a starting offset bits away from the MSB of register element 1
        """
        self._ensure_valid_nary_operation(start_index, end_index, element_width, stride, register_a)

        result = 0
        result_bits = 0
        pseudo_sew = min(element_width, self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8)
        elements = math.ceil((end_index - start_index) // element_width)
        for element in range(elements):
            if element % (stride // element_width) == 0:  # is this sew chunk to be operated on due to the stride?
                result <<= 1
                result += byte_array_to_int(
                    self.registers[register_a][element * pseudo_sew:element * pseudo_sew + pseudo_sew]
                )
                result_bits += 1

        result <<= self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8 - result_bits - bit_offset
        self.registers[register_a] = int_to_byte_array(
            result,
            self.bank_hardware.hardware_configuration.row_buffer_size_bytes
        )

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to begin the algo
        # Calculate how many sew_chunks there are
        elements = math.ceil((end_index - start_index) / element_width)
        # Calculate how many source operands there are
        operands = elements // (stride // pseudo_sew)
        # Calculate how many stride-SEW ALU rounds are needed
        alu_rounds = operands
        # each ALU requires at least an SLL and an addition
        cycles += alu_rounds * 2
        # it takes result_bits / 8 memory stores
        cycles += result_bits // 8

        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a} <- BITMAP[{register_a}]",
            return_labels=return_labels
        )

    def blimp_bit_count(self, register, start_index, end_index, element_width, return_labels=True) -> RuntimeResult:
        """
        Count the number of set bits in a register and save it in the primary slot
        """
        self._ensure_valid_nary_operation(start_index, end_index, element_width, element_width, register)

        count = 0
        blimp_cycles = 1  # vector dispatch
        pseudo_sew = min(element_width, self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8)
        elements = math.ceil((end_index - start_index) // element_width)

        blimp_cycles += 2  # loop start
        for element in range(elements):
            blimp_cycles += 2  # register read, loop setup
            value = byte_array_to_int(
                self.registers[register][element * pseudo_sew:element * pseudo_sew + pseudo_sew]
            )
            while value:
                value &= value - 1
                count += 1
                blimp_cycles += 4  # subtraction, and, addition, jump

        count <<= self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8 - pseudo_sew * 8
        self.registers[register] = int_to_byte_array(
            count,
            self.bank_hardware.hardware_configuration.row_buffer_size_bytes
        )

        return self.blimp_cycle(
            cycles=blimp_cycles,
            label=f"\t{register} <- COUNT[{register}]",
            return_labels=return_labels
        )

    def blimp_bit_popcount(self, register, start_index, end_index, element_width, return_labels=True) -> RuntimeResult:
        """
        Pop-count the number of set bits in a register and save it in the primary slot
        """
        self._ensure_valid_nary_operation(start_index, end_index, element_width, element_width, register)

        count = 0
        blimp_cycles = 1  # dispatch
        pseudo_sew = min(element_width, self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8)
        elements = math.ceil((end_index - start_index) // element_width)
        blimp_cycles += elements * 2

        for element in range(elements):
            value = byte_array_to_int(
                self.registers[register][element * pseudo_sew:element * pseudo_sew + pseudo_sew]
            )
            while value:
                value &= value - 1
                count += 1

        count <<= self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8 - pseudo_sew * 8
        self.registers[register] = int_to_byte_array(
            count,
            self.bank_hardware.hardware_configuration.row_buffer_size_bytes
        )

        return self.blimp_cycle(
            cycles=blimp_cycles,
            label=f"\t{register} <- COUNT[{register}]",
            return_labels=return_labels
        )


class SimulatedBlimpVBank(SimulatedBlimpBank):
    """Defines simulation parameters for a BLIMP-V-capable DRAM Bank"""
    def __init__(
            self,
            bank_hardware: BlimpVectorBank,
            logger=None
            ):
        super(SimulatedBlimpVBank, self).__init__(bank_hardware, logger)
        self.bank_hardware = bank_hardware

        self._logger.info(f"blimpv simulator loaded")

    def _blimpv_set_register_to_(self, ones: bool, register, return_labels=True) -> RuntimeResult:
        """Set the specified register to all zeros or ones"""

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine
        # Calculate how many sew_chunks there are
        sew_chunks = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // \
            self.bank_hardware.hardware_configuration.blimpv_sew_max_bytes
        # Calculate how many source operands there are
        operands = sew_chunks
        # Calculate how many stride-SEW ALU rounds are needed
        alu_rounds = int(math.ceil(operands / self.bank_hardware.hardware_configuration.number_of_vALUs))
        # Assumption; each ALU takes less than a CPU cycle to execute
        cycles += alu_rounds

        result = self.blimp_cycle(
            label=f"\t{register} <- {int(ones)}",
            cycles=cycles,
            return_labels=return_labels
        )

        self.registers[register] = \
            int_to_byte_array(
                (2 ** (self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8)) - 1 if ones else 0,
                self.bank_hardware.hardware_configuration.row_buffer_size_bytes
            )

        return result

    def blimpv_set_scratchpad_to_zero(self, return_labels=True) -> RuntimeResult:
        """Set the data scratchpad to all zeros"""
        return self._blimpv_set_register_to_(False, self.blimp_data_scratchpad, return_labels)

    def blimpv_set_scratchpad_to_one(self, return_labels=True) -> RuntimeResult:
        """Set the data scratchpad to all ones"""
        return self._blimpv_set_register_to_(True, self.blimp_data_scratchpad, return_labels)

    def blimpv_set_v1_to_zero(self, return_labels=True) -> RuntimeResult:
        """Set v1 to all zeros"""
        return self._blimpv_set_register_to_(False, self.blimp_v1, return_labels)

    def blimpv_set_v1_to_one(self, return_labels=True) -> RuntimeResult:
        """Set v1 to all ones"""
        return self._blimpv_set_register_to_(True, self.blimp_v1, return_labels)

    def blimpv_set_v2_to_zero(self, return_labels=True) -> RuntimeResult:
        """Set v2 to all zeros"""
        return self._blimpv_set_register_to_(False, self.blimp_v2, return_labels)

    def blimpv_set_v2_to_one(self, return_labels=True) -> RuntimeResult:
        """Set v2 to all ones"""
        return self._blimpv_set_register_to_(True, self.blimp_v2, return_labels)

    def _ensure_valid_v_nary_operation(self, sew, stride, *registers):
        self._ensure_writable_register(*registers)
        assert self.bank_hardware.hardware_configuration.blimpv_sew_min_bytes <= sew, "SEW too small for this configuration"
        assert self.bank_hardware.hardware_configuration.blimpv_sew_max_bytes >= sew, "SEW too large for this configuration"
        assert self.bank_hardware.hardware_configuration.row_buffer_size_bytes % sew == 0, f"SEW of {sew} does not divide evenly into the configured row buffer width"
        assert stride % sew == 0, f"Stride of {stride} must be a direct multiple of the SEW {sew}"

    def _blimpv_alu_unary_operation(self, register_a, sew, stride, operation, invert):
        """Perform a BLIMP-V unary operation and store the result in Register A"""
        # Sanity checking
        self._ensure_valid_v_nary_operation(sew, stride, register_a)

        result = 0
        for sew_chunk in range(self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew):
            a = byte_array_to_int(self.registers[register_a][sew_chunk*sew:sew_chunk*sew+sew])

            if sew_chunk % (stride // sew) == 0:  # is this sew chunk to be operated on due to the stride?
                c = operation(a)
                c = ((~c if invert else c) & (2 ** (sew * 8)) - 1)  # eliminate any carry's and invert if needed
            else:
                c = a  # just keep what was there
            result <<= sew * 8
            result += c

        self.registers[register_a] = int_to_byte_array(
            result, self.bank_hardware.hardware_configuration.row_buffer_size_bytes)

    def _blimpv_alu_binary_operation(self, register_a, register_b, sew, stride, operation, invert):
        """Perform a BLIMP-V binary operation and store the result in Register B"""
        # Sanity checking
        self._ensure_valid_v_nary_operation(sew, stride, register_a, register_b)

        result = 0
        for sew_chunk in range(self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew):
            a = byte_array_to_int(self.registers[register_a][sew_chunk*sew:sew_chunk*sew+sew])
            b = byte_array_to_int(self.registers[register_b][sew_chunk*sew:sew_chunk*sew+sew])

            if sew_chunk % (stride // sew) == 0:  # is this sew chunk to be operated on due to the stride?
                c = operation(a, b)
                c = ((~c if invert else c) & (2 ** (sew * 8)) - 1)  # eliminate any carry's and invert if needed
            else:
                c = a  # just keep what was there in the source register
            result <<= sew * 8
            result += c

        self.registers[register_b] = int_to_byte_array(
            result, self.bank_hardware.hardware_configuration.row_buffer_size_bytes)

    def _blimpv_alu_int_un_op(self, register_a, sew, stride, operation, invert, op_name, return_labels=True) \
            -> RuntimeResult:
        """Perform a BLIMP-V unary operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        self._blimpv_alu_unary_operation(
            register_a,
            sew,
            stride,
            operation,
            invert
        )

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine
        # Calculate how many sew_chunks there are
        sew_chunks = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        # Calculate how many source operands there are
        operands = sew_chunks // (stride // sew)
        # Calculate how many stride-SEW ALU rounds are needed
        alu_rounds = int(math.ceil(operands / self.bank_hardware.hardware_configuration.number_of_vALUs))
        # Assumption; each ALU takes less than a CPU cycle to execute
        cycles += alu_rounds

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a} <- {op_name} {register_a}",
            return_labels=return_labels
        )

    def _blimpv_alu_int_bin_op(self, register_a, register_b, sew, stride, operation, invert, op_name, return_labels=True
                               ) -> RuntimeResult:
        """Perform a BLIMP-V binary operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        self._blimpv_alu_binary_operation(
            register_a,
            register_b,
            sew,
            stride,
            operation,
            invert
        )

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine
        # Calculate how many sew_chunks there are
        sew_chunks = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        # Calculate how many source operands there are
        operands = sew_chunks // (stride // sew)
        # Calculate how many stride-SEW ALU rounds are needed
        alu_rounds = int(math.ceil(operands / self.bank_hardware.hardware_configuration.number_of_vALUs))
        # Assumption; each ALU takes less than a CPU cycle to execute
        cycles += alu_rounds

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_b} <- {register_a} {op_name} {register_b}",
            return_labels=return_labels
        )

    def blimpv_alu_int_and(self, register_a, register_b, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V AND operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            stride,
            lambda a, b: a & b,
            False,
            "AND",
            return_labels
        )

    def blimpv_alu_int_or(self, register_a, register_b, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V OR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            stride,
            lambda a, b: a | b,
            False,
            "OR",
            return_labels
        )

    def blimpv_alu_int_xor(self, register_a, register_b, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V XOR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            stride,
            lambda a, b: a ^ b,
            False,
            "XOR",
            return_labels
        )

    def blimpv_alu_int_nand(self, register_a, register_b, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NAND operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            stride,
            lambda a, b: a & b,
            True,
            "NAND",
            return_labels
        )

    def blimpv_alu_int_nor(self, register_a, register_b, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NOR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            stride,
            lambda a, b: a | b,
            True,
            "NOR",
            return_labels
        )

    def blimpv_alu_int_xnor(self, register_a, register_b, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V XNOR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            stride,
            lambda a, b: a ^ b,
            True,
            "XNOR",
            return_labels
        )

    def blimpv_alu_int_add(self, register_a, register_b, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ADD operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            stride,
            lambda a, b: a + b,
            False,
            "ADD",
            return_labels
        )

    def blimpv_alu_int_sub(self, register_a, register_b, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V SUB operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            stride,
            lambda a, b: a - b,
            False,
            "SUB",
            return_labels
        )

    def blimpv_alu_int_not(self, register_a, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NOT operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: ~a,
            False,
            "NOT",
            return_labels
        )

    def blimpv_alu_int_acc(self, register_a, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ACC operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: a + 1,
            False,
            "ACC",
            return_labels
        )

    def blimpv_alu_int_zero(self, register_a, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ISZERO operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: int(a == 0),
            False,
            "ZERO",
            return_labels
        )

    def blimpv_alu_int_one(self, register_a, sew, stride, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ISONE operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: int(a == 2**sew - 1),
            False,
            "ONE",
            return_labels
        )

    def blimpv_alu_int_xnor_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP-V scalar XNOR operation on register 'a' on SEW bytes against constant 'value' and store the
        result in register a
        """
        # Perform the operation
        assert 0 <= value <= 2**(sew*8) - 1, "Constant-Sew bit-width mismatch"
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: a ^ value,
            True,
            "XNOR",
            return_labels
        )

    def blimpv_alu_int_and_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP-V scalar AND operation on register 'a' on SEW bytes against constant 'value' and store the
        result in register a
        """
        # Perform the operation
        assert 0 <= value <= 2**(sew*8) - 1, "Constant-Sew bit-width mismatch"
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: a & value,
            False,
            "AND",
            return_labels
        )

    def blimpv_alu_int_srl_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V SRL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: a >> value,
            False,
            "SRL",
            return_labels
        )

    def blimpv_alu_int_sll_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V SLL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: a << value,
            False,
            "SLL",
            return_labels
        )

    def blimpv_alu_int_mul_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V MULTIPLY operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: a * value,
            False,
            "MUL",
            return_labels
        )

    def blimpv_alu_int_eq_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V EQUAL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: int(a == value),
            False,
            "EQ",
            return_labels
        )

    def blimpv_alu_int_neq_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NOT EQUAL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: int(a != value),
            False,
            "NEQ",
            return_labels
        )

    def blimpv_alu_int_lt_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V LESS THAN operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: int(a < value),
            False,
            "LT",
            return_labels
        )

    def blimpv_alu_int_gt_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V GREATER THAN operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: int(a > value),
            False,
            "GT",
            return_labels
        )

    def blimpv_alu_int_lte_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP-V LESS THAN OR EQUAL operation on a register on SEW bytes and store the result in register a
        """
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: int(a <= value),
            False,
            "LTE",
            return_labels
        )

    def blimpv_alu_int_gte_val(self, register_a, sew, stride, value, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP-V GREATER THAN OR EQUAL operation on a register on SEW bytes and store the result in register a
        """
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            stride,
            lambda a: int(a >= value),
            False,
            "GTE",
            return_labels
        )

    def blimpv_coalesce_register_hitmap(self, register_a, sew, stride, bit_offset, return_labels=True) -> RuntimeResult:
        """
        Coalesce a bitmap in a register starting offset bits away from the MSB of register element 1
        """
        self._ensure_valid_v_nary_operation(sew, stride, register_a)

        result = 0
        result_bits = 0
        for sew_chunk in range(self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew):
            if sew_chunk % (stride // sew) == 0:  # is this sew chunk to be operated on due to the stride?
                result <<= 1
                result += byte_array_to_int(self.registers[register_a][sew_chunk * sew:sew_chunk * sew + sew])
                result_bits += 1

        result <<= self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8 - result_bits - bit_offset
        self.registers[register_a] = int_to_byte_array(
            result,
            self.bank_hardware.hardware_configuration.row_buffer_size_bytes
        )

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine
        # Calculate how many sew_chunks there are
        sew_chunks = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        # Calculate how many source operands there are
        operands = sew_chunks // (stride // sew)
        # Calculate how many stride-SEW ALU rounds are needed
        alu_rounds = int(math.ceil(operands / self.bank_hardware.hardware_configuration.number_of_vALUs))
        # each ALU requires at least an SLL and an addition
        cycles += alu_rounds * 2
        # it takes result_bits / 8 memory stores
        cycles += result_bits // 8

        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a} <- BITMAP[{register_a}]",
            return_labels=return_labels
        )

    def blimpv_bit_popcount(self, register_a, sew, stride, return_labels=True) -> RuntimeResult:
        """
        Pop-count the number of set bits in a register and save it in the primary slot
        """
        self._ensure_valid_v_nary_operation(sew, stride, register_a)

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine
        # Calculate how many sew_chunks there are
        sew_chunks = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        # Calculate how many source operands there are
        operands = sew_chunks // (stride // sew)
        # Calculate how many stride-SEW ALU rounds are needed
        alu_rounds = int(math.ceil(operands / self.bank_hardware.hardware_configuration.number_of_vALUs))
        # Assumption; each ALU takes less than a CPU cycle to execute
        cycles += alu_rounds

        count = 0
        for sew_chunk in range(self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew):
            value = byte_array_to_int(self.registers[register_a][sew_chunk * sew:sew_chunk * sew + sew])
            while value:
                value &= value - 1
                count += 1

        count <<= self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8 - sew * 8
        self.registers[register_a] = int_to_byte_array(
            count,
            self.bank_hardware.hardware_configuration.row_buffer_size_bytes
        )

        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a} <- COUNT[{register_a}]",
            return_labels=return_labels
        )
