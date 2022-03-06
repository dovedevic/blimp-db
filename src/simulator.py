import os
import logging

from src.configuration import SystemConfiguration
from src.record_generator import BaseRecordGenerator
from src.bank import Bank
from src.result import ResultSet


class SimulatedBank:
    """
    Assumes Row 0 through config_max are Ambit control rows, with the first two being static rows
    """
    def __init__(
                self,
                name: str,
                config: SystemConfiguration,
                bank: Bank=None,
                records: BaseRecordGenerator=None,
                perform_placement=True
            ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.name = name
        self.logger.info("initializing simulator...")

        self.logger.info("performing configuration calculations...")
        self.config = config.calculate_rows()

        self.logger.info(
            "loading simulator with prebuilt bank" if bank else
            "loading simulator with null bank, creating...")
        self.bank = bank or Bank(config.bank_size, config.row_buffer_size)

        self.logger.info(
            "loading simulator with prebuilt record corpus" if records else
            "loading simulator with null corpus, generating...")
        self.record_corpus = records or BaseRecordGenerator(
            int(config.record_to_rb_ratio * config.row_buffer_size),
            config.total_index_size,
            config.bank_size
        )

        # Check if this is an ambit bank
        self.logger.info("laying out bank with ambit controls...")
        if self.config.ambit_control_rows >= 2:
            self.bank.set_zeros(0)
            self.bank.set_ones(1)
            self.bank.set_ones(2)
            self.bank.set_ones(3)
            self.bank.set_ones(4)
            self.bank.set_ones(5)

        self.ambit_control_base_row = 0
        self.ambit_compute_base_row = self.ambit_control_base_row + self.config.ambit_control_rows
        self.record_base_row = self.config.ambit_control_rows + self.config.total_available_rows_for_ambit
        self.hitmap_base_row = self.record_base_row + self.config.total_available_rows_for_data

        if not perform_placement:
            self.logger.info("bank loaded")
        else:
            self.do_bank_prep()
            self.logger.info("bank prepped and loaded")

    def do_bank_prep(self):
        # Load the records into the database
        self.logger.info("laying out bank with data records...")
        self.logger.info(f">> ambit base row -- {self.ambit_control_base_row}")
        self.logger.info(f">> record base row -- {self.record_base_row}")
        current_bank_row_index = self.record_base_row
        max_bank_row_index = self.record_base_row + self.config.total_available_rows_for_data
        current_built_row = ""
        records_processed = 0
        for record in self.record_corpus.get_raw_records():
            # Only continue processing if we have available rows
            if current_bank_row_index >= max_bank_row_index:
                break
            records_processed += 1

            # Build the record
            current_built_row += record

            # Is this record less than the row buffer size?
            if len(current_built_row) < self.config.row_buffer_size * 8:
                # If so continue to build the record
                continue
            # Is this record equal to the row buffer size?
            elif len(current_built_row) == self.config.row_buffer_size * 8:
                # If so set the record
                self.bank.set_raw_row(current_bank_row_index, int(current_built_row, 2))
                current_bank_row_index += 1
                current_built_row = ""
            # Is this record greater than the row buffer size?
            else:
                # Check how many rows this consumes, ensure we have the space, otherwise breakout
                num_rows_for_record = len(current_built_row) // (self.config.row_buffer_size * 8)
                if current_bank_row_index + num_rows_for_record >= max_bank_row_index:
                    break

                # If it consumes available rows, chunk the record into them
                for i in range(0, len(current_built_row), self.config.row_buffer_size * 8):
                    row_chunk = current_built_row[i:i + self.config.row_buffer_size * 8]

                    # Post-pad the record with zeros if needed
                    row_chunk += "0" * ((self.config.row_buffer_size * 8) - len(row_chunk))

                    # Add this row
                    self.bank.set_raw_row(current_bank_row_index, int(row_chunk, 2))
                    current_bank_row_index += 1
                current_built_row = ""
        self.logger.info(f"placed {records_processed} records")

        # Load the P/I fields into the ambit rows
        self.logger.info("laying out bank with ambit p/i fields...")
        self.logger.info(f">> ambit base row -- {self.ambit_compute_base_row}")
        for i in range(self.config.total_available_rows_for_ambit):
            # Calculate meta-specifics
            record_page, sub_record_index = i // (self.config.total_index_size * 8), \
                                            i % (self.config.total_index_size * 8)
            start_record = record_page * self.config.row_buffer_size * 8

            # Generate a new constructed row based on the current meta-specifics
            row = ""
            for j in range(self.config.row_buffer_size * 8):
                # Ensure we are placing the same number of records in ambit rows as we are in data
                if start_record + j < records_processed:
                    # Fetch a record from the bank based on it's index
                    row += self.record_corpus.get_raw_record(start_record + j)[sub_record_index]
                else:
                    # If we reach our limit, null pad the rest of ambit rows
                    row += '0'

            # Set this new translated row into the bank
            self.bank.set_raw_row(self.ambit_compute_base_row + i, int(row, 2))

        # Initialize the hitmap
        self.logger.info("laying out hitmap region...")

        self.logger.info(f">> hitmap base row -- {self.hitmap_base_row}")
        for i in range(self.config.total_available_rows_for_hitmap):
            row = ('1' * self.config.row_buffer_size * 8)[:records_processed - (i * self.config.row_buffer_size * 8)]
            row += '0' * (self.config.row_buffer_size * 8 - len(row))
            self.bank.set_raw_row(self.hitmap_base_row + i, int(row, 2))

    def save(self, suffix="", config=True, bank=True, records=True):
        self.logger.info(f"saving simulation configuration as: " + f"simulations/{self.name}")
        base_path = f"simulations/{self.name}"
        if not os.path.exists(base_path):
            os.mkdir(base_path)
        if config:
            self.config.save(self.name + suffix, prefix=base_path)
        if bank:
            self.bank.save(self.name + suffix, prefix=base_path)
        if records:
            self.record_corpus.save(self.name + suffix, prefix=base_path)

    def checkpoint_bank(self, name):
        self.save(suffix=f"-{name}-checkpoint", config=False, records=False)

    def _ambit_0(self):
        return self.ambit_control_base_row + 0

    def _ambit_1(self):
        return self.ambit_control_base_row + 1

    def _ambit_control(self):
        return self.ambit_control_base_row + 2

    def _ambit_temp_a(self):
        return self.ambit_control_base_row + 3

    def _ambit_temp_b(self):
        return self.ambit_control_base_row + 4

    def _ambit_temp_c(self):
        return self.ambit_control_base_row + 5

    def _ambit_temp_d(self):
        return self.ambit_control_base_row + 6

    def _ambit_and(self, a, b) -> ResultSet:
        self.bank.copy_row(self._ambit_0(), self._ambit_control())
        self.bank.maj_rows(a, b, self._ambit_control())
        return ResultSet(self.config).copy().tra()

    def _ambit_nand(self, a, b) -> ResultSet:
        self.bank.copy_row(self._ambit_0(), self._ambit_control())
        self.bank.maj_rows(a, b, self._ambit_control(), invert=True)
        return ResultSet(self.config).copy().tra()

    def _ambit_or(self, a, b) -> ResultSet:
        self.bank.copy_row(self._ambit_1(), self._ambit_control())
        self.bank.maj_rows(a, b, self._ambit_control())
        return ResultSet(self.config).copy().tra()

    def _ambit_nor(self, a, b) -> ResultSet:
        self.bank.copy_row(self._ambit_1(), self._ambit_control())
        self.bank.maj_rows(a, b, self._ambit_control(), invert=True)
        return ResultSet(self.config).copy().tra()

    def _ambit_not(self, a) -> ResultSet:
        self.bank.invert_row(a)
        return ResultSet(self.config).copy().copy()

    def perform_ambit_equal_query(self, pi_subindex_offset, pi_element_size, value) -> ResultSet:
        return self._perform_ambit_equality_query(pi_subindex_offset, pi_element_size, value, False)

    def perform_ambit_not_equal_query(self, pi_subindex_offset, pi_element_size, value) -> ResultSet:
        return self._perform_ambit_equality_query(pi_subindex_offset, pi_element_size, value, True)

    def _perform_ambit_equality_query(self, pi_subindex_offset, pi_element_size, value, negate) -> ResultSet:
        result = ResultSet(self.config)
        result.cycle()
        if isinstance(value, list):
            value = [0] * (pi_element_size * 8 - len(value)) + value
        else:
            value = [int(b) for b in str(bin(value))[2:]][:pi_element_size * 8]
            value = [0] * (pi_element_size * 8 - len(value)) + value

        result.cycle()
        for h in range(self.config.total_available_rows_for_hitmap):  # Iterate over all hitmap rows
            result.cycle()
            for b in range(pi_element_size * 8):  # Iterate over the bits per element
                result.cycle()

                # Calculate the row offset to fetch
                row_to_check = self.ambit_compute_base_row + \
                               h * self.config.total_index_size * 8 + \
                               pi_subindex_offset * 8 + \
                               b
                result.cycle(cycles=8)

                # dup this row to save it
                self.bank.copy_row(row_to_check, self._ambit_temp_a())
                result.copy()

                # dup a control row for this bit
                if value[b]:
                    self.bank.copy_row(self._ambit_1(), self._ambit_temp_b())
                else:
                    self.bank.copy_row(self._ambit_0(), self._ambit_temp_b())
                result.cycle(cycles=2).copy()

                # perform pi_index an AND
                result += self._ambit_and(self._ambit_temp_a(), self._ambit_temp_b())
                # SAVE temp_b = pi AND bit

                # dup this row to save it
                self.bank.copy_row(row_to_check, self._ambit_temp_a())
                result.copy()

                # dup a control row for this bit
                if value[b]:
                    self.bank.copy_row(self._ambit_1(), self._ambit_temp_c())
                else:
                    self.bank.copy_row(self._ambit_0(), self._ambit_temp_c())
                result.cycle(cycles=2).copy()

                # perform pi_index an NOR
                result += self._ambit_nor(self._ambit_temp_a(), self._ambit_temp_c())
                # SAVE temp_c = pi NOR bit

                # perform pi_index an OR
                result += self._ambit_or(self._ambit_temp_b(), self._ambit_temp_c())
                # SAVE temp_b = pi XNOR bit

                # dup hitmap row to compute
                self.bank.copy_row(self.hitmap_base_row + h, self._ambit_temp_a())
                result.copy()

                # perform hitmap AND
                result += self._ambit_and(self._ambit_temp_b(), self._ambit_temp_a())

                # move compute to hitmap row
                self.bank.copy_row(self._ambit_temp_a(), self.hitmap_base_row + h)
                result.copy()

            # Check if this operation requires the hitmap result to be inverted
            if negate:
                result.cycle()
                # dup hitmap row to compute
                self.bank.copy_row(self.hitmap_base_row + h, self._ambit_temp_a())
                result.copy()

                # negate and move to hitmap row
                self.bank.copy_row(self._ambit_temp_a(), self.hitmap_base_row + h, invert=True)
                result.copy()
            result.cycle()
        return result
