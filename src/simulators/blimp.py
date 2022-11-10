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

    def blimp_load_register(self, register, row: int, return_labels=True) -> RuntimeResult:
        """Fetch data into a specified internal register, via v0"""
        # Sanity checking
        if register not in self.registers:
            raise RuntimeError(f"Register '{register}' does not exist")
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

    def blimp_get_register(self, register) -> [int]:
        """Fetch the data for a BLIMP or BLIMP-V register"""
        if register not in self.registers:
            raise RuntimeError(f"Register '{register}' does not exist")
        return self.registers[register]

    def blimp_save_register(self, register, row: int, return_labels=True) -> RuntimeResult:
        """Save a specified internal register into a row, via v0"""
        # Sanity checking
        if register not in self.registers:
            raise RuntimeError(f"Register '{register}' does not exist")
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

    def _blimp_set_scratchpad_to_(self, ones: bool, return_labels=True) -> RuntimeResult:
        """Set the data scratchpad to all zeros or ones"""
        total_bits = self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8
        processor_bits = self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture
        operations = int(math.ceil(total_bits / processor_bits))
        cycles_per_operation = 2  # mov x <- ones | jne i

        result = self.blimp_cycle(label="; loop start", return_labels=return_labels)
        result += self.blimp_cycle(
            cycles=operations * cycles_per_operation
        )

        self.registers[self.blimp_data_scratchpad] = \
            int_to_byte_array(
                (2**(self.bank_hardware.hardware_configuration.row_buffer_size_bytes * 8)) - 1 if ones else 0,
                self.bank_hardware.hardware_configuration.row_buffer_size_bytes
            )

        return result

    def blimp_set_scratchpad_to_zero(self, return_labels=True) -> RuntimeResult:
        """Set the data scratchpad to all zeros"""
        return self._blimp_set_scratchpad_to_(False, return_labels)

    def blimp_set_scratchpad_to_one(self, return_labels=True) -> RuntimeResult:
        """Set the data scratchpad to all ones"""
        return self._blimp_set_scratchpad_to_(True, return_labels)

    def _blimp_alu_unary_operation(self, register_a, start_byte_index, end_byte_index, operation, invert):
        """Perform a BLIMP unary operation and store the result in Register A"""
        # Sanity checking
        if register_a not in self.registers:
            raise RuntimeError(f"Register '{register_a}' does not exist")
        elif end_byte_index < 0 or start_byte_index < 0:
            raise RuntimeError("Byte indices must be positive")
        elif end_byte_index > self.bank_hardware.hardware_configuration.row_buffer_size_bytes:
            raise RuntimeError("End byte indices must be smaller than the row buffer size")
        elif end_byte_index < start_byte_index:
            raise RuntimeError("end_byte_index must come after start_byte_index")
        elif end_byte_index - start_byte_index > self.bank_hardware.hardware_configuration.row_buffer_size_bytes:
            raise RuntimeError(f"Byte index range exceeds hardware capability")

        word_size = self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8
        words = math.ceil((end_byte_index - start_byte_index) / word_size)

        for word in range(words):
            a = byte_array_to_int(
                self.registers
                [register_a]
                [word * word_size + start_byte_index: word * word_size + start_byte_index + word_size]
            )
            c = operation(a)
            c = ((~c if invert else c) & (2 ** (word_size * 8)) - 1)  # eliminate any carry's and invert if needed
            c = int_to_byte_array(c, word_size)
            for byte in range(word_size):
                self.registers[register_a][word * word_size + start_byte_index + byte] = c[byte]

    def _blimp_alu_binary_operation(self, register_a, register_b, start_byte_index, end_byte_index, operation, invert):
        """Perform a BLIMP-V binary operation and store the result in Register B"""
        # Sanity checking
        if register_a not in self.registers:
            raise RuntimeError(f"Register '{register_a}' does not exist")
        elif register_b not in self.registers:
            raise RuntimeError(f"Register '{register_b}' does not exist")
        elif end_byte_index < 0 or start_byte_index < 0:
            raise RuntimeError("Byte indices must be positive")
        elif end_byte_index > self.bank_hardware.hardware_configuration.row_buffer_size_bytes:
            raise RuntimeError("End byte indices must be smaller than the row buffer size")
        elif end_byte_index < start_byte_index:
            raise RuntimeError("end_byte_index must come after start_byte_index")
        elif end_byte_index - start_byte_index > self.bank_hardware.hardware_configuration.row_buffer_size_bytes:
            raise RuntimeError(f"Byte index range exceeds hardware capability")

        word_size = self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8
        words = math.ceil((end_byte_index - start_byte_index) / word_size)

        for word in range(words):
            a = byte_array_to_int(
                self.registers
                  [register_a]
                    [word * word_size + start_byte_index : word * word_size + start_byte_index + word_size]
            )
            b = byte_array_to_int(
                self.registers
                  [register_b]
                    [word * word_size + start_byte_index : word * word_size + start_byte_index + word_size]
            )
            c = operation(a, b)
            c = ((~c if invert else c) & (2 ** (word_size * 8)) - 1)  # eliminate any carry's and invert if needed
            c = int_to_byte_array(c, word_size)
            for byte in range(word_size):
                self.registers[register_b][word * word_size + start_byte_index + byte] = c[byte]

    def _blimp_alu_int_un_op(self, register_a, start_byte_index, end_byte_index, operation, invert, op_name,
                             return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP unary operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        self._blimp_alu_unary_operation(
            register_a,
            start_byte_index,
            end_byte_index,
            operation,
            invert
        )

        # Calculate the number of cycles this operation takes
        cycles = 1
        # Calculate how many words we are processing
        words = math.ceil(
            (end_byte_index - start_byte_index) /
            (self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8)
        )
        # Calculate how many ALU rounds are needed
        alu_rounds = words
        # Assumption; ALU takes a CPU cycle to execute, at least once extra cycle for looping per alu round
        cycles += alu_rounds * 2

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a}[{start_byte_index}:{end_byte_index}] <- {op_name} {register_a}[{start_byte_index}:{end_byte_index}]",
            return_labels=return_labels
        )

    def _blimp_alu_int_bin_op(self, register_a, register_b, start_byte_index, end_byte_index, operation, invert,
                              op_name, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP binary operation on register a and b starting at a specified byte index and ending on a
        specified byte index then store the result in register b
        """
        # Perform the operation
        self._blimp_alu_binary_operation(
            register_a,
            register_b,
            start_byte_index,
            end_byte_index,
            operation,
            invert
        )

        # Calculate the number of cycles this operation takes
        cycles = 1
        # Calculate how many words we are processing
        words = math.ceil(
            (end_byte_index - start_byte_index) /
            (self.bank_hardware.hardware_configuration.blimp_processor_bit_architecture // 8)
        )
        # Calculate how many ALU rounds are needed
        alu_rounds = words
        # Assumption; ALU takes a CPU cycle to execute, at least once extra cycle for looping per alu round
        cycles += alu_rounds * 2

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_b}[{start_byte_index}:{end_byte_index}] <- {register_a}[{start_byte_index}:{end_byte_index}] {op_name} {register_b}[{start_byte_index}:{end_byte_index}]",
            return_labels=return_labels
        )

    def blimp_alu_int_and(self, register_a, register_b, start_byte_index, end_byte_index, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP AND operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_byte_index,
            end_byte_index,
            lambda a, b: a & b,
            False,
            "AND",
            return_labels
        )

    def blimp_alu_int_or(self, register_a, register_b, start_byte_index, end_byte_index, return_labels=True
                         ) -> RuntimeResult:
        """
        Perform a BLIMP OR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_byte_index,
            end_byte_index,
            lambda a, b: a | b,
            False,
            "OR",
            return_labels
        )

    def blimp_alu_int_xor(self, register_a, register_b, start_byte_index, end_byte_index, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP XOR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_byte_index,
            end_byte_index,
            lambda a, b: a ^ b,
            False,
            "XOR",
            return_labels
        )

    def blimp_alu_int_nand(self, register_a, register_b, start_byte_index, end_byte_index, return_labels=True
                           ) -> RuntimeResult:
        """
        Perform a BLIMP NAND operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_byte_index,
            end_byte_index,
            lambda a, b: a & b,
            True,
            "NAND",
            return_labels
        )

    def blimp_alu_int_nor(self, register_a, register_b, start_byte_index, end_byte_index, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP NOR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_byte_index,
            end_byte_index,
            lambda a, b: a | b,
            True,
            "NOR",
            return_labels
        )

    def blimp_alu_int_xnor(self, register_a, register_b, start_byte_index, end_byte_index, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP XNOR operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_byte_index,
            end_byte_index,
            lambda a, b: a ^ b,
            True,
            "XNOR",
            return_labels
        )

    def blimp_alu_int_add(self, register_a, register_b, start_byte_index, end_byte_index, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP ADD operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_byte_index,
            end_byte_index,
            lambda a, b: a + b,
            False,
            "ADD",
            return_labels
        )

    def blimp_alu_int_sub(self, register_a, register_b, start_byte_index, end_byte_index, return_labels=True
                          ) -> RuntimeResult:
        """
        Perform a BLIMP SUB operation on register a and b starting at a specified byte index and ending on a specified
        byte index then store the result in register b
        """
        # Perform the operation
        return self._blimp_alu_int_bin_op(
            register_a,
            register_b,
            start_byte_index,
            end_byte_index,
            lambda a, b: a - b,
            False,
            "SUB",
            return_labels
        )

    def blimp_alu_int_not(self, register_a, start_byte_index, end_byte_index, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP NOT operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_byte_index,
            end_byte_index,
            lambda a: ~a,
            False,
            "NOT",
            return_labels
        )

    def blimp_alu_int_acc(self, register_a, start_byte_index, end_byte_index, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP ACC operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_byte_index,
            end_byte_index,
            lambda a: a + 1,
            False,
            "ACC",
            return_labels
        )

    def blimp_alu_int_zero(self, register_a, start_byte_index, end_byte_index, return_labels=True) -> RuntimeResult:
        """
        Perform a BLIMP ISZERO operation on register a starting at a specified byte index and ending on a specified
        byte index then store the result in register a
        """
        # Perform the operation
        return self._blimp_alu_int_un_op(
            register_a,
            start_byte_index,
            end_byte_index,
            lambda a: int(a == 0),
            False,
            "ZERO",
            return_labels
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

    def _blimpv_alu_unary_operation(self, register_a, sew, operation, invert):
        """Perform a BLIMP-V unary operation and store the result in Register A"""
        # Sanity checking
        if register_a not in self.registers:
            raise RuntimeError(f"Register '{register_a}' does not exist")
        elif self.bank_hardware.hardware_configuration.blimpv_sew_min_bytes > sew:
            raise RuntimeError("SEW too small for this configuration")
        elif self.bank_hardware.hardware_configuration.blimpv_sew_max_bytes < sew:
            raise RuntimeError("SEW too large for this configuration")
        elif self.bank_hardware.hardware_configuration.row_buffer_size_bytes % sew != 0:
            raise RuntimeError(f"SEW of {sew} does not divide evenly into the configured row buffer width")

        result = 0
        for sew_chunk in range(self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew):
            a = byte_array_to_int(self.registers[register_a][sew_chunk*sew:sew_chunk*sew+sew])
            c = operation(a)
            c = ((~c if invert else c) & (2 ** (sew * 8)) - 1)  # eliminate any carry's and invert if needed
            result <<= sew * 8
            result += c

        self.registers[register_a] = int_to_byte_array(
            result, self.bank_hardware.hardware_configuration.row_buffer_size_bytes)

    def _blimpv_alu_binary_operation(self, register_a, register_b, sew, operation, invert):
        """Perform a BLIMP-V binary operation and store the result in Register B"""
        # Sanity checking
        if register_a not in self.registers:
            raise RuntimeError(f"Register '{register_a}' does not exist")
        elif register_b not in self.registers:
            raise RuntimeError(f"Register '{register_b}' does not exist")
        elif self.bank_hardware.hardware_configuration.blimpv_sew_min_bytes > sew:
            raise RuntimeError("SEW too small for this configuration")
        elif self.bank_hardware.hardware_configuration.blimpv_sew_max_bytes < sew:
            raise RuntimeError("SEW too large for this configuration")
        elif self.bank_hardware.hardware_configuration.row_buffer_size_bytes % sew != 0:
            raise RuntimeError(f"SEW of {sew} does not divide evenly into the configured row buffer width")

        result = 0
        for sew_chunk in range(self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew):
            a = byte_array_to_int(self.registers[register_a][sew_chunk*sew:sew_chunk*sew+sew])
            b = byte_array_to_int(self.registers[register_b][sew_chunk*sew:sew_chunk*sew+sew])
            c = operation(a, b)
            c = ((~c if invert else c) & (2 ** (sew * 8)) - 1)  # eliminate any carry's and invert if needed
            result <<= sew * 8
            result += c

        self.registers[register_b] = int_to_byte_array(
            result, self.bank_hardware.hardware_configuration.row_buffer_size_bytes)

    def _blimpv_alu_int_un_op(self, register_a, sew, operation, invert, op_name, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V unary operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        self._blimpv_alu_unary_operation(
            register_a,
            sew,
            operation,
            invert
        )

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine
        # Calculate how many sew_chunks there are
        sew_chunks = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        # Calculate how many SEW ALU rounds are needed
        alu_rounds = int(math.ceil(sew_chunks / self.bank_hardware.hardware_configuration.number_of_vALUs))
        # Assumption; each ALU takes less than a CPU cycle to execute
        cycles += alu_rounds

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a} <- {op_name} {register_a}",
            return_labels=return_labels
        )

    def _blimpv_alu_int_bin_op(self, register_a, register_b, sew, operation, invert, op_name, return_labels=True
                               ) -> RuntimeResult:
        """Perform a BLIMP-V binary operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        self._blimpv_alu_binary_operation(
            register_a,
            register_b,
            sew,
            operation,
            invert
        )

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine
        # Calculate how many sew_chunks there are
        sew_chunks = self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew
        # Calculate how many SEW ALU rounds are needed
        alu_rounds = int(math.ceil(sew_chunks / self.bank_hardware.hardware_configuration.number_of_vALUs))
        # Assumption; each ALU takes less than a CPU cycle to execute
        cycles += alu_rounds

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_b} <- {register_a} {op_name} {register_b}",
            return_labels=return_labels
        )

    def blimpv_alu_int_and(self, register_a, register_b, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V AND operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            lambda a, b: a & b,
            False,
            "AND",
            return_labels
        )

    def blimpv_alu_int_or(self, register_a, register_b, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V OR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            lambda a, b: a | b,
            False,
            "OR",
            return_labels
        )

    def blimpv_alu_int_xor(self, register_a, register_b, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V XOR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            lambda a, b: a ^ b,
            False,
            "XOR",
            return_labels
        )

    def blimpv_alu_int_nand(self, register_a, register_b, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NAND operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            lambda a, b: a & b,
            True,
            "NAND",
            return_labels
        )

    def blimpv_alu_int_nor(self, register_a, register_b, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NOR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            lambda a, b: a | b,
            True,
            "NOR",
            return_labels
        )

    def blimpv_alu_int_xnor(self, register_a, register_b, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V XNOR operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            lambda a, b: a ^ b,
            True,
            "XNOR",
            return_labels
        )

    def blimpv_alu_int_add(self, register_a, register_b, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ADD operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            lambda a, b: a + b,
            False,
            "ADD",
            return_labels
        )

    def blimpv_alu_int_sub(self, register_a, register_b, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V SUB operation on register a and b on SEW bytes and store the result in register b"""
        # Perform the operation
        return self._blimpv_alu_int_bin_op(
            register_a,
            register_b,
            sew,
            lambda a, b: a - b,
            False,
            "SUB",
            return_labels
        )

    def blimpv_alu_int_not(self, register_a, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V NOT operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            lambda a: ~a,
            False,
            "NOT",
            return_labels
        )

    def blimpv_alu_int_acc(self, register_a, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ACC operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            lambda a: a + 1,
            False,
            "ACC",
            return_labels
        )

    def blimpv_alu_int_zero(self, register_a, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V ISZERO operation on register 'a' on SEW bytes and store the result in register a"""
        # Perform the operation
        return self._blimpv_alu_int_un_op(
            register_a,
            sew,
            lambda a: int(a == 0),
            False,
            "ZERO",
            return_labels
        )

    def blimpv_alu_int_max(self, register_a, sew, return_labels=True) -> RuntimeResult:
        """Perform a BLIMP-V MAX operation on register 'a' on SEW bytes and store the result in register a"""
        # Sanity checking
        if register_a not in self.registers:
            raise RuntimeError(f"Register '{register_a}' does not exist")
        elif self.bank_hardware.hardware_configuration.blimpv_sew_min_bytes > sew:
            raise RuntimeError("SEW too small for this configuration")
        elif self.bank_hardware.hardware_configuration.blimpv_sew_max_bytes < sew:
            raise RuntimeError("SEW too large for this configuration")
        elif self.bank_hardware.hardware_configuration.row_buffer_size_bytes % sew != 0:
            raise RuntimeError(f"SEW of {sew} does not divide evenly into the configured row buffer width")

        # Calculate the number of cycles this operation takes
        cycles = 1  # Start with one cycle to dispatch to the vector engine

        # Perform the operation
        reduction_rounds = math.floor(math.log2(self.bank_hardware.hardware_configuration.row_buffer_size_bytes // sew))
        for reduction_round in range(reduction_rounds):
            element_pairs = self.bank_hardware.hardware_configuration.row_buffer_size_bytes \
                            // sew \
                            // (2**(reduction_round + 1))

            # Calculate how many SEW ALU rounds are needed
            alu_rounds = int(math.ceil(element_pairs / self.bank_hardware.hardware_configuration.number_of_vALUs))
            # Assumption; each ALU takes less than a CPU cycle to execute
            cycles += alu_rounds

            for pair_index in range(element_pairs):
                pair_neighbor_distance = 2 ** reduction_round
                left_pair_index = pair_index * 2 * 2 ** (reduction_round + 1)
                right_pair_index = left_pair_index + pair_neighbor_distance

                left_pair_byte_offset = left_pair_index * sew
                right_pair_byte_offset = right_pair_index * sew

                a = byte_array_to_int(self.registers[register_a][left_pair_byte_offset:left_pair_byte_offset+sew])
                b = byte_array_to_int(self.registers[register_a][right_pair_byte_offset:right_pair_byte_offset+sew])
                c = max(a, b)

                # Set the left to the max
                self.registers[register_a][left_pair_byte_offset:left_pair_byte_offset + sew] = \
                    int_to_byte_array(c, sew)

                # Set the right to zero
                self.registers[register_a][right_pair_byte_offset:right_pair_byte_offset + sew] = [0] * sew

        # At this point register[0] begins the max sew element

        # Return the runtime result
        return self.blimp_cycle(
            cycles=cycles,
            label=f"\t{register_a}[0] <- MAX {register_a}",
            return_labels=return_labels
        )
