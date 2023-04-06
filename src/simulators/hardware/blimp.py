import math

from src.hardware.architectures import BlimpBank, BlimpVectorBank
from src.simulators.simulator import SimulatedBank
from src.simulators.result import RuntimeResult
from src.utils.bitmanip import byte_array_to_int, int_to_byte_array, msb_bit


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
            self._v3(): [],
            self._v4(): [],
            self._v5(): [],
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
    def _v3():
        """Register name for the third BLIMP-V vector register"""
        return "v3"

    @staticmethod
    def _v4():
        """Register name for the fourth BLIMP-V vector register"""
        return "v4"

    @staticmethod
    def _v5():
        """Register name for the fifth BLIMP-V vector register"""
        return "v5"

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
    def blimp_v3(self):
        """BLIMP-V V3 vector register"""
        return self._v3()

    @property
    def blimp_v4(self):
        """BLIMP-V V4 vector register"""
        return self._v4()

    @property
    def blimp_v5(self):
        """BLIMP-V V5 vector register"""
        return self._v5()

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
        assert element_width > 0, \
            "Element widths must be positive"
        assert end_index >= 0 and start_index >= 0, \
            "Byte indices must be positive"
        assert end_index <= self.bank_hardware.hardware_configuration.row_buffer_size_bytes, \
            "End byte indices must be smaller than the row buffer size"
        assert end_index >= start_index, \
            "end_index must come after start_index"
        assert end_index - start_index <= self.bank_hardware.hardware_configuration.row_buffer_size_bytes, \
            "Byte index range exceeds hardware capability"
        assert element_width <= word_size, \
            "Element width must be smaller than this processors word size"
        assert word_size % element_width == 0, \
            "Element width must be a multiple of the word size"
        assert stride % element_width == 0, \
            f"Stride of {stride} must be a direct multiple of the element size {element_width}"

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
        self.registers[register_b] = [b for b in self.registers[register_a]]

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

    def blimp_get_register_data(self, register, element_width) -> [int]:
        """Fetch the data for a BLIMP or BLIMP-V register in python/segmented form"""
        self._ensure_register_exists(register)

        data = []
        raw_data_bytes = self.registers[register]
        for sew_chunk in range(self.bank_hardware.hardware_configuration.row_buffer_size_bytes // element_width):
            data.append(
                byte_array_to_int(raw_data_bytes[sew_chunk*element_width:sew_chunk*element_width+element_width])
            )
        return data

    def blimp_set_register_data_at_index(self, register, element_width, index, value, return_labels=True,
                                         assume_one_cycle=False):
        """Set an element in a BLIMP or BLIMP-V register in python/segmented form"""
        self._ensure_register_exists(register)
        assert 0 <= index < (self.bank_hardware.hardware_configuration.row_buffer_size_bytes // element_width), \
            "set index is out of range"
        assert 0 <= value < 2 ** (element_width * 8), f"given value is not representable by {element_width} bytes"

        value_bytes = int_to_byte_array(value, element_width)
        for byte_index, vb in enumerate(value_bytes):
            self.registers[register][index * element_width + byte_index] = vb

        if not assume_one_cycle:
            result = self.blimp_cycle(
                cycles=math.ceil(
                    element_width / (self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8)
                ),
                return_labels=return_labels
            )
        else:
            result = self.blimp_cycle(return_labels=return_labels)

        # Return the result of the operation
        return result

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

    def blimp_set_register_to_zero(self, register, return_labels=True) -> RuntimeResult:
        """Set an arbitrary register to all zeros"""
        self._ensure_writable_register(register)
        return self._blimp_set_register_to_(False, register, return_labels)

    def blimp_set_register_to_one(self, register, return_labels=True) -> RuntimeResult:
        """Set an arbitrary register to all ones"""
        self._ensure_writable_register(register)
        return self._blimp_set_register_to_(True, register, return_labels)

    def blimp_is_register_zero(self, register) -> bool:
        self._ensure_register_exists(register)
        # This can be done for free if our registers have a ZF
        return byte_array_to_int(self.registers[register]) == 0

    def _blimp_alu_unary_operation(self, register_a, start_index, end_index, element_width, stride, operation, invert,
                                   mask):
        """Perform a BLIMP unary operation and store the result in Register A"""
        # Sanity checking
        self._ensure_valid_nary_operation(start_index, end_index, element_width, stride, register_a)

        elements = math.ceil((end_index - start_index) // element_width)
        max_maskable_bits = elements // (stride // element_width)
        max_maskable_value = (2 ** max_maskable_bits) - 1
        mask = mask if mask != -1 else max_maskable_value
        assert 0 <= mask <= max_maskable_value, f"the mask does not equally represent {max_maskable_bits} elements"

        for element in range(elements):
            a = byte_array_to_int(
                self.registers
                [register_a]
                [element * element_width + start_index: element * element_width + start_index + element_width]
            )

            # is this sew chunk to be operated on due to the stride? If it is, has it been masked out?
            if element % (stride // element_width) == 0 and msb_bit(mask, element, max_maskable_bits):
                c = operation(a)
                c = ((~c if invert else c) & (2 ** (element_width * 8)) - 1)  # eliminate any carry's, invert if needed
            else:
                c = a  # just keep what was there
            c = int_to_byte_array(c, element_width)
            for byte in range(element_width):
                self.registers[register_a][element * element_width + start_index + byte] = c[byte]

    def _blimp_alu_binary_operation(self, register_a, register_b, start_index, end_index, element_width, stride,
                                    operation, invert, mask):
        """Perform a BLIMP-V binary operation and store the result in Register B"""
        # Sanity checking
        self._ensure_valid_nary_operation(start_index, end_index, element_width, stride, register_a, register_b)

        elements = math.ceil((end_index - start_index) // element_width)
        max_maskable_bits = elements // (stride // element_width)
        max_maskable_value = (2 ** max_maskable_bits) - 1
        mask = mask if mask != -1 else max_maskable_value
        assert 0 <= mask <= max_maskable_value, f"the mask does not equally represent {max_maskable_bits} elements"

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

            # is this sew chunk to be operated on due to the stride? If it is, has it been masked out?
            if element % (stride // element_width) == 0 and msb_bit(mask, element, max_maskable_bits):
                c = operation(a, b)
                c = ((~c if invert else c) & (2 ** (element_width * 8)) - 1)  # eliminate any carry's, invert if needed
            else:
                c = a  # just keep what was in the source register
            c = int_to_byte_array(c, element_width)
            for byte in range(element_width):
                self.registers[register_b][element * element_width + start_index + byte] = c[byte]

    def _blimp_alu_int_un_op(self, register_a, start_index, end_index, element_width, stride, operation, invert,
                             op_name, mask=-1, cpi=1, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP unary operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        self._blimp_alu_unary_operation(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=operation,
            invert=invert,
            mask=mask
        )

        # Calculate the number of cycles this operation takes
        cycles = 1
        # Calculate how many elements we are processing
        elements = math.ceil((end_index - start_index) / element_width)
        # Calculate how many source operands there are
        operands = elements // (stride // element_width)

        if mask != -1:  # if the mask was not set, we are assuming this is a for loop, otherwise it's a masked loop
            # Calculate how many operands are operated on
            operated = bin(mask).count("1")
            # Calculate how many ALU rounds are needed
            alu_rounds = operated
            # Add cycles to do the bit checks, & and BNE
            cycles += operands * 2
        else:
            alu_rounds = operands

        # Assumption; ALU takes a CPU cycle to execute, at least once extra cycle for looping per alu round
        cycles += alu_rounds * cpi * 2

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a}[{start_index}:{end_index}] <- {op_name} {register_a}[{start_index}:{end_index}]",
            return_labels=return_labels
        )

    def _blimp_alu_int_bin_op(self, register_a, register_b, start_index, end_index, element_width, stride, operation,
                              invert, op_name, mask=-1, cpi=1, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP binary operation on register a and b starting at a specified byte index and ending on a
        specified byte index then store the result in register b
        """
        # Perform the operation
        self._blimp_alu_binary_operation(
            register_a=register_a,
            register_b=register_b,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=operation,
            invert=invert,
            mask=mask
        )

        # Calculate the number of cycles this operation takes
        cycles = 1
        # Calculate how many words we are processing
        elements = math.ceil((end_index - start_index) / element_width)
        # Calculate how many source operands there are
        operands = elements // (stride // element_width)

        if mask != -1:  # if the mask was not set, we are assuming this is a for loop, otherwise it's a masked loop
            # Calculate how many operands are operated on
            operated = bin(mask).count("1")
            # Calculate how many ALU rounds are needed
            alu_rounds = operated
            # Add cycles to do the bit checks, & and BNE
            cycles += operands * 2
        else:
            alu_rounds = operands

        # Assumption; ALU takes a CPU cycle to execute, at least once extra cycle for looping per alu round
        cycles += alu_rounds * cpi * 2

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_b}[{start_index}:{end_index}] <- "
                  f"{register_a}[{start_index}:{end_index}] {op_name} {register_b}[{start_index}:{end_index}]",
            return_labels=return_labels
        )

    def blimp_alu_int_and(self, register_a, register_b, start_index, end_index, element_width, stride, mask=-1,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP AND operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a, b: a & b,
            invert=False,
            op_name="AND",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_or(self, register_a, register_b, start_index, end_index, element_width, stride, mask=-1,
                         return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP OR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a, b: a | b,
            invert=False,
            op_name="OR",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_xor(self, register_a, register_b, start_index, end_index, element_width, stride, mask=-1,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP XOR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a, b: a ^ b,
            invert=False,
            op_name="XOR",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_nand(self, register_a, register_b, start_index, end_index, element_width, stride, mask=-1,
                           return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP NAND operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a, b: a & b,
            invert=True,
            op_name="NAND",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_nor(self, register_a, register_b, start_index, end_index, element_width, stride, mask=-1,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP NOR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a, b: a | b,
            invert=True,
            op_name="NOR",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_xnor(self, register_a, register_b, start_index, end_index, element_width, stride, mask=-1,
                           return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP XNOR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a, b: a ^ b,
            invert=True,
            op_name="XNOR",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_add(self, register_a, register_b, start_index, end_index, element_width, stride, mask=-1,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP ADD operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a, b: a + b,
            invert=False,
            op_name="ADD",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_sub(self, register_a, register_b, start_index, end_index, element_width, stride, mask=-1,
                          return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP SUB operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a, b: a - b,
            invert=False,
            op_name="SUB",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_not(self, register_a, start_index, end_index, element_width, stride, mask=-1, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP NOT operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: ~a,
            invert=False,
            op_name="NOT",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_acc(self, register_a, start_index, end_index, element_width, stride, mask=-1, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP ACC operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: a + 1,
            invert=False,
            op_name="ACC",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_zero(self, register_a, start_index, end_index, element_width, stride, mask=-1, return_labels=True
                           ) -> RuntimeResult:
        """
        Perform a BLIMP ISZERO operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: int(a == 0),
            invert=False,
            op_name="ZERO",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_xnor_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                               return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP scalar XNOR operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        assert 0 <= value <= 2 ** self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture - 1, \
            "Scalar Operation Value bit-width mismatch"
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: a ^ value,
            invert=True,
            op_name="XNOR",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_and_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                              return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP scalar AND operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        assert 0 <= value <= 2 ** self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture - 1, \
            "Scalar Operation Value bit-width mismatch"
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: a & value,
            invert=False,
            op_name="AND",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_eq_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                             return_labels=True) -> RuntimeResult:
        """Perform a BLIMP EQUAL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: int(a == value),
            invert=False,
            op_name="EQ",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_neq_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                              return_labels=True) -> RuntimeResult:
        """Perform a BLIMP NOT EQUAL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: int(a != value),
            invert=False,
            op_name="NEQ",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_lt_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                             return_labels=True) -> RuntimeResult:
        """Perform a BLIMP LESS THAN operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: int(a < value),
            invert=False,
            op_name="LT",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_gt_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                             return_labels=True) -> RuntimeResult:
        """Perform a BLIMP GREATER THAN operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: int(a > value),
            invert=False,
            op_name="GT",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_lte_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                              return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP LESS THAN OR EQUAL operation on a register on SEW bytes and store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: int(a <= value),
            invert=False,
            op_name="LTE",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_gte_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                              return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP GREATER THAN OR EQUAL operation on a register on SEW bytes and store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: int(a >= value),
            invert=False,
            op_name="GTE",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_add_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                              return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP scalar ADD operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: a + value,
            invert=False,
            op_name="ADD",
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_mul_val(self, register_a, start_index, end_index, element_width, stride, value, mask=-1,
                              return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP scalar MULTIPLY operation on register a starting at a specified byte index and ending on a
        specified byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: a * value,
            invert=False,
            op_name="MUL",
            cpi=3,
            mask=mask,
            return_labels=return_labels
        )

    def blimp_alu_int_hash(self, register_a, start_index, end_index, element_width, stride, hash_mask, mask=-1,
                           return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP scalar HASH(val) operation on register a starting at a specified byte index and ending on a
        specified byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a=register_a,
            start_index=start_index,
            end_index=end_index,
            element_width=element_width,
            stride=stride,
            operation=lambda a: ((a * 3634946921) + 2096170329) & hash_mask,
            invert=False,
            op_name="HASH",
            cpi=3+1+1,
            mask=mask,
            return_labels=return_labels
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
            label=f"\t{register} <- POPCOUNT[{register}]",
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

    def blimpv_set_register_to_zero(self, register, return_labels=True) -> RuntimeResult:
        """Set an arbitrary register to all zeros"""
        self._ensure_writable_register(register)
        return self._blimpv_set_register_to_(False, register, return_labels)

    def blimpv_set_register_to_one(self, register, return_labels=True) -> RuntimeResult:
        """Set an arbitrary register to all ones"""
        self._ensure_writable_register(register)
        return self._blimpv_set_register_to_(True, register, return_labels)

    def _ensure_valid_v_nary_operation(self, sew, stride, *registers):
        self._ensure_writable_register(*registers)
        assert self.bank_hardware.hardware_configuration.blimpv_sew_min_bytes <= sew, \
            "SEW too small for this configuration"
        assert self.bank_hardware.hardware_configuration.blimpv_sew_max_bytes >= sew, \
            "SEW too large for this configuration"
        assert self.bank_hardware.hardware_configuration.row_buffer_size_bytes % sew == 0, \
            f"SEW of {sew} does not divide evenly into the configured row buffer width"
        assert stride % sew == 0, \
            f"Stride of {stride} must be a direct multiple of the SEW {sew}"

    def _blimpv_alu_unary_operation(self, register_a, sew, stride, operation, invert, mask):
        """Perform a BLIMP-V unary operation and store the result in Register A"""
        # Sanity checking
        self._ensure_valid_v_nary_operation(sew, stride, register_a)

        elements = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        max_maskable_bits = elements // (stride // sew)
        max_maskable_value = (2 ** max_maskable_bits) - 1
        mask = mask if mask != -1 else max_maskable_value
        assert 0 <= mask <= max_maskable_value, f"the mask does not equally represent {max_maskable_bits} elements"

        result = 0
        for sew_chunk in range(elements):
            a = byte_array_to_int(self.registers[register_a][sew_chunk*sew:sew_chunk*sew+sew])

            # is this sew chunk to be operated on due to the stride? If it is, has it been masked out?
            if sew_chunk % (stride // sew) == 0 and msb_bit(mask, sew_chunk, max_maskable_bits):
                c = operation(a)
                c = ((~c if invert else c) & (2 ** (sew * 8)) - 1)  # eliminate any carry's and invert if needed
            else:
                c = a  # just keep what was there
            result <<= sew * 8
            result += c

        self.registers[register_a] = int_to_byte_array(
            result, self.bank_hardware.hardware_configuration.row_buffer_size_bytes)

    def _blimpv_alu_binary_operation(self, register_a, register_b, sew, stride, operation, invert, mask):
        """Perform a BLIMP-V binary operation and store the result in Register B"""
        # Sanity checking
        self._ensure_valid_v_nary_operation(sew, stride, register_a, register_b)

        elements = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        max_maskable_bits = elements // (stride // sew)
        max_maskable_value = (2 ** max_maskable_bits) - 1
        mask = mask if mask != -1 else max_maskable_value
        assert 0 <= mask <= max_maskable_value, f"the mask does not equally represent {max_maskable_bits} elements"

        result = 0
        for sew_chunk in range(elements):
            a = byte_array_to_int(self.registers[register_a][sew_chunk*sew:sew_chunk*sew+sew])
            b = byte_array_to_int(self.registers[register_b][sew_chunk*sew:sew_chunk*sew+sew])

            # is this sew chunk to be operated on due to the stride? If it is, has it been masked out?
            if sew_chunk % (stride // sew) == 0 and msb_bit(mask, sew_chunk, max_maskable_bits):
                c = operation(a, b)
                c = ((~c if invert else c) & (2 ** (sew * 8)) - 1)  # eliminate any carry's and invert if needed
            else:
                c = a  # just keep what was there in the source register
            result <<= sew * 8
            result += c

        self.registers[register_b] = int_to_byte_array(
            result, self.bank_hardware.hardware_configuration.row_buffer_size_bytes)

    def _blimpv_alu_int_un_op(self, register_a, sew, stride, operation, invert, op_name, mask, cpi=1,
                              return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V unary operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        self._blimpv_alu_unary_operation(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=operation,
            invert=invert,
            mask=mask
        )

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine
        # Calculate how many sew_chunks there are
        sew_chunks = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        # Calculate how many source operands there are
        operands = sew_chunks // (stride // sew)

        if mask != -1:  # if the mask was not set, we are assuming this is a for loop, otherwise it's a masked loop
            # Calculate how many operands are operated on
            operated = bin(mask).count("1")
            # Calculate how many ALU rounds are needed
            alu_rounds = int(math.ceil(operated / self.bank_hardware.hardware_configuration.number_of_vALUs))
        else:
            # Calculate how many stride-SEW ALU rounds are needed
            alu_rounds = int(math.ceil(operands / self.bank_hardware.hardware_configuration.number_of_vALUs))

        # each ALU takes CPI cycles to execute
        cycles += alu_rounds * cpi

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a} <- {op_name} {register_a}",
            return_labels=return_labels
        )

    def _blimpv_alu_int_bin_op(self, register_a, register_b, sew, stride, operation, invert, op_name, mask, cpi=1,
                               return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V binary operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        self._blimpv_alu_binary_operation(
            register_a=register_a,
            register_b=register_b,
            sew=sew,
            stride=stride,
            operation=operation,
            invert=invert,
            mask=mask
        )

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine
        # Calculate how many sew_chunks there are
        sew_chunks = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        # Calculate how many source operands there are
        operands = sew_chunks // (stride // sew)

        if mask != -1:  # if the mask was not set, we are assuming this is a for loop, otherwise it's a masked loop
            # Calculate how many operands are operated on
            operated = bin(mask).count("1")
            # Calculate how many ALU rounds are needed
            alu_rounds = int(math.ceil(operated / self.bank_hardware.hardware_configuration.number_of_vALUs))
        else:
            # Calculate how many stride-SEW ALU rounds are needed
            alu_rounds = int(math.ceil(operands / self.bank_hardware.hardware_configuration.number_of_vALUs))

        # each ALU takes CPI cycles to execute
        cycles += alu_rounds * cpi

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_b} <- {register_a} {op_name} {register_b}",
            return_labels=return_labels
        )

    def blimpv_alu_int_and(self, register_a, register_b, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V AND operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            sew=sew,
            stride=stride,
            operation=lambda a, b: a & b,
            invert=False,
            op_name="AND",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_or(self, register_a, register_b, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V OR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            sew=sew,
            stride=stride,
            operation=lambda a, b: a | b,
            invert=False,
            op_name="OR",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_xor(self, register_a, register_b, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V XOR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            sew=sew,
            stride=stride,
            operation=lambda a, b: a ^ b,
            invert=False,
            op_name="XOR",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_nand(self, register_a, register_b, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NAND operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            sew=sew,
            stride=stride,
            operation=lambda a, b: a & b,
            invert=True,
            op_name="NAND",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_nor(self, register_a, register_b, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NOR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            sew=sew,
            stride=stride,
            operation=lambda a, b: a | b,
            invert=True,
            op_name="NOR",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_xnor(self, register_a, register_b, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V XNOR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            sew=sew,
            stride=stride,
            operation=lambda a, b: a ^ b,
            invert=True,
            op_name="XNOR",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_add(self, register_a, register_b, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ADD operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            sew=sew,
            stride=stride,
            operation=lambda a, b: a + b,
            invert=False,
            op_name="ADD",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_sub(self, register_a, register_b, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V SUB operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a=register_a,
            register_b=register_b,
            sew=sew,
            stride=stride,
            operation=lambda a, b: a - b,
            invert=False,
            op_name="SUB",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_not(self, register_a, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NOT operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: ~a,
            invert=False,
            op_name="NOT",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_acc(self, register_a, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ACC operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: a + 1,
            invert=False,
            op_name="ACC",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_zero(self, register_a, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ISZERO operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: int(a == 0),
            invert=False,
            op_name="ZERO",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_one(self, register_a, sew, stride, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ISONE operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: int(a == 2**sew - 1),
            invert=False,
            op_name="ONE",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_xnor_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP-V scalar XNOR operation on register 'a' on SEW bytes against constant 'value' and store the
        result in register a
        """
        # Perform the operation
        assert 0 <= value <= 2**(sew*8) - 1, "Constant-Sew bit-width mismatch"
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: a ^ value,
            invert=True,
            op_name="XNOR",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_and_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP-V scalar AND operation on register 'a' on SEW bytes against constant 'value' and store the
        result in register a
        """
        # Perform the operation
        assert 0 <= value <= 2**(sew*8) - 1, "Constant-Sew bit-width mismatch"
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: a & value,
            invert=False,
            op_name="AND",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_srl_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V SRL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: a >> value,
            invert=False,
            op_name="SRL",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_sll_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V SLL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: a << value,
            invert=False,
            op_name="SLL",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_add_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ADD operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: a + value,
            invert=False,
            op_name="ADD",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_mul_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V MULTIPLY operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: a * value,
            invert=False,
            op_name="MUL",
            cpi=3,
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_eq_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V EQUAL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: int(a == value),
            invert=False,
            op_name="EQ",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_neq_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NOT EQUAL operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: int(a != value),
            invert=False,
            op_name="NEQ",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_lt_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V LESS THAN operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: int(a < value),
            invert=False,
            op_name="LT",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_gt_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V GREATER THAN operation on a register on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: int(a > value),
            invert=False,
            op_name="GT",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_lte_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP-V LESS THAN OR EQUAL operation on a register on SEW bytes and store the result in register a
        """
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: int(a <= value),
            invert=False,
            op_name="LTE",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_gte_val(self, register_a, sew, stride, value, mask=-1, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP-V GREATER THAN OR EQUAL operation on a register on SEW bytes and store the result in register a
        """
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: int(a >= value),
            invert=False,
            op_name="GTE",
            mask=mask,
            return_labels=return_labels
        )

    def blimpv_alu_int_hash(self, register_a, sew, stride, hash_mask, mask=-1, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP-V HASH operation on a register on SEW bytes and store the result in register a
        """
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a=register_a,
            sew=sew,
            stride=stride,
            operation=lambda a: ((a * 3634946921) + 2096170329) & hash_mask,
            invert=False,
            op_name="HASH",
            mask=mask,
            cpi=3+1+1,
            return_labels=return_labels
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
            label=f"\t{register_a} <- POPCOUNT[{register_a}]",
            return_labels=return_labels
        )
