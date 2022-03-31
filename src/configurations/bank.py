import math

from src.configurations.system import AmbitSystemConfiguration, BlimpSystemConfiguration, SystemConfiguration
from src.configurations.user import AmbitDatabaseConfiguration, BlimpDatabaseConfiguration, DatabaseConfiguration


class BankLayoutConfiguration:
    """Defines the row layout configuration for a standard DRAM database bank"""
    def __init__(self, system: SystemConfiguration, user: DatabaseConfiguration):
        self._system_configuration = system
        self._user_configuration = user

    def display(self):
        """Dump the configuration into the console for visual inspection"""
        print("System Configuration:")
        self._system_configuration.display()

        print("User Configuration:")
        self._user_configuration.display()

    @property
    def address_mapping(self):
        """Return the row address mapping for this configuration"""
        mapping = dict()
        return mapping

    @property
    def system_configuration(self):
        """Get the internal system configuration"""
        return self._system_configuration

    @property
    def database_configuration(self):
        """Get the internal user-defined database configuration"""
        return self._user_configuration


class AmbitBankLayoutConfiguration(BankLayoutConfiguration):
    """Defines the row layout configuration for an AMBIT-BLIMP database bank"""
    def __init__(self, system: AmbitSystemConfiguration, user: AmbitDatabaseConfiguration):
        super().__init__(system, user)

        self._system_configuration = system
        self._user_configuration = user

        # Ambit compute region rows, B and C groups
        self.total_rows_for_reserved_ambit_compute = self._system_configuration.ambit_control_rows

        # User-defined temporary rows (ambit D group) for ambit calculations
        self.total_rows_for_temporary_ambit_compute = self._user_configuration.ambit_temporary_bits

        # User-defined rows dedicated to storing BLIMP compute code
        self.total_rows_for_blimp_code_region = int(math.ceil(self._user_configuration.blimp_code_region_size_bytes
                                                    / self._system_configuration.row_buffer_size_bytes))

        # Total rows to play with when configuring the layout
        self.total_rows_for_configurable_data = self._system_configuration.bank_rows \
            - self.total_rows_for_reserved_ambit_compute \
            - self.total_rows_for_temporary_ambit_compute \
            - self.total_rows_for_blimp_code_region

        if self.total_rows_for_configurable_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static row constraints")

        if self._user_configuration.total_record_size_bytes > self._system_configuration.row_buffer_size_bytes:
            if self._user_configuration.total_record_size_bytes % self._system_configuration.row_buffer_size_bytes != 0:
                raise ValueError("Record sizes must be row buffer aligned to at least a power of two")
        else:
            if self._system_configuration.row_buffer_size_bytes % self._user_configuration.total_record_size_bytes != 0:
                raise ValueError("Record sizes must be row buffer aligned to at least a power of two")

        pi_rows = 0
        data_rows = 0
        hitmap_rows = 0
        processable_records = 0
        record_to_row_buffer_ratio = self._user_configuration.total_record_size_bytes \
            / self._system_configuration.row_buffer_size_bytes
        while pi_rows + data_rows + hitmap_rows < self.total_rows_for_configurable_data:
            # PI Rows are calculated by the number of bits an index field is in length, due to the vertical layout
            new_pi_rows = self._user_configuration.total_index_size_bytes * 8
            # Hitmap rows are calculated one bit per PI field, per each hitmap
            new_hitmap_rows = self._user_configuration.hitmap_count
            # Data rows are calculated by the row buffer width of records, multiplied by the size of the record
            # rb * 8 * data / rb
            new_data_rows = 8 * self._user_configuration.total_record_size_bytes

            # Can we fit a full set of records into the bank?
            if new_pi_rows + pi_rows + \
                    new_hitmap_rows + hitmap_rows + \
                    new_data_rows + data_rows < self.total_rows_for_configurable_data:
                # If we can, add this new block into our existing set
                pi_rows += new_pi_rows
                hitmap_rows += new_hitmap_rows
                data_rows += new_data_rows
                processable_records += self._system_configuration.row_buffer_size_bytes * 8
                continue

            # Can we fit a subset of records into the bank?
            elif new_pi_rows + pi_rows + \
                    new_hitmap_rows + hitmap_rows + \
                    data_rows < self.total_rows_for_configurable_data:
                # If we can, ensure we can add at least one data record
                if record_to_row_buffer_ratio >= 1 and (new_pi_rows + pi_rows + new_hitmap_rows + hitmap_rows +
                   data_rows + record_to_row_buffer_ratio) > self.total_rows_for_configurable_data:
                    # If we can't fit at least one record, break out
                    break

                # At this point at least one record is placeable, so place the blocks
                pi_rows += new_pi_rows
                hitmap_rows += new_hitmap_rows

                rows_remaining = self.total_rows_for_configurable_data - pi_rows - hitmap_rows - data_rows
                if record_to_row_buffer_ratio <= 1:
                    records_per_row = self._system_configuration.row_buffer_size_bytes \
                                      // self._user_configuration.total_record_size_bytes
                    processable_records += rows_remaining * records_per_row
                    data_rows += rows_remaining
                else:
                    records_in_remainder = rows_remaining // (
                            self._user_configuration.total_record_size_bytes
                            // self._system_configuration.row_buffer_size_bytes
                    )
                    processable_records += records_in_remainder
                    data_rows += records_in_remainder * (
                            self._user_configuration.total_record_size_bytes
                            // self._system_configuration.row_buffer_size_bytes
                    )
                break

            # Can no more blocks of rows can be placed
            else:
                break

        # Done heuristically placing rows in bank, finalize configuration

        # Total rows for Ambit-format pi-field(s)
        self.total_rows_for_ambit_pi_field = pi_rows

        # Total rows for BLIMP-format records (pi field + data / k + v)
        self.total_rows_for_records = data_rows

        # Total rows for Ambit/BLIMP hitmap placement
        self.total_rows_for_hitmaps = hitmap_rows

        # Total number of records operable with this configuration
        self.total_records_processable = processable_records

        # Ensure we are inbounds
        if pi_rows + data_rows + hitmap_rows > self.total_rows_for_configurable_data:
            raise AssertionError("Heuristic placement failed, alter parameters or reserved rows")

    def display(self):
        """Dump the configuration into the console for visual inspection"""
        print("System Configuration:")
        self._system_configuration.display()

        print("User Configuration:")
        self._user_configuration.display()

        print("Bank Meta Parameters")
        print(f"    total_rows_for_configurable_data:       {self.total_rows_for_configurable_data}")
        print(f"    total_records_processable:              {self.total_records_processable}")

        print("Bank Row Mappings:")
        print(f"    total_rows_for_blimp_code_region:       {self.total_rows_for_blimp_code_region}")
        print(f"    total_rows_for_ambit_pi_field:          {self.total_rows_for_ambit_pi_field}")
        print(f"    total_rows_for_temporary_ambit_compute: {self.total_rows_for_temporary_ambit_compute}")
        print(f"    total_rows_for_records:                 {self.total_rows_for_records}")
        print(f"    total_rows_for_hitmaps:                 {self.total_rows_for_hitmaps}")
        print(f"    total_rows_for_reserved_ambit_compute:  {self.total_rows_for_reserved_ambit_compute}")

    def address_mapping(self):
        """Return the row address mapping for this configuration"""
        base = 0
        mapping = dict()

        # BLIMP Code region always starts at row 0
        mapping["blimp_code_region"] = [base, self.total_rows_for_blimp_code_region]
        base += self.total_rows_for_blimp_code_region

        mapping["ambit_pi_field"] = [base, self.total_rows_for_ambit_pi_field]
        base += self.total_rows_for_ambit_pi_field

        mapping["temporary_ambit_compute"] = [base, self.total_rows_for_temporary_ambit_compute]
        base += self.total_rows_for_temporary_ambit_compute

        mapping["records"] = [base, self.total_rows_for_records]
        base += self.total_rows_for_records

        mapping["hitmaps"] = [base, self.total_rows_for_hitmaps]
        base += self.total_rows_for_hitmaps

        # Ambit compute always resides at the last rows
        mapping["reserved_ambit_compute"] = [
            self._system_configuration.bank_rows - self.total_rows_for_reserved_ambit_compute,
            self._system_configuration.bank_rows
        ]

        return mapping


class BlimpBankLayoutConfiguration(BankLayoutConfiguration):
    """Defines the row layout configuration for an BLIMP database bank"""
    def __init__(self, system: BlimpSystemConfiguration, user: BlimpDatabaseConfiguration):
        super().__init__(system, user)

        self._system_configuration = system
        self._user_configuration = user

        # User-defined rows dedicated to storing BLIMP compute code
        self.total_rows_for_blimp_code_region = int(math.ceil(self._user_configuration.blimp_code_region_size_bytes
                                                    / self._system_configuration.row_buffer_size_bytes))

        # Total rows to play with when configuring the layout
        self.total_rows_for_configurable_data = self._system_configuration.bank_rows \
            - self.total_rows_for_blimp_code_region

        if self.total_rows_for_configurable_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static row constraints")

        if self._user_configuration.total_record_size_bytes > self._system_configuration.row_buffer_size_bytes:
            if self._user_configuration.total_record_size_bytes % self._system_configuration.row_buffer_size_bytes != 0:
                raise ValueError("Record sizes must be row buffer aligned to at least a power of two")
        else:
            if self._system_configuration.row_buffer_size_bytes % self._user_configuration.total_record_size_bytes != 0:
                raise ValueError("Record sizes must be row buffer aligned to at least a power of two")

        data_rows = 0
        hitmap_rows = 0
        processable_records = 0
        record_to_row_buffer_ratio = self._user_configuration.total_record_size_bytes \
            / self._system_configuration.row_buffer_size_bytes
        while data_rows + hitmap_rows < self.total_rows_for_configurable_data:
            # Hitmap rows are calculated one bit per PI field, per each hitmap
            new_hitmap_rows = self._user_configuration.hitmap_count
            # Data rows are calculated by the row buffer width of records, multiplied by the size of the record
            # rb * 8 * data / rb
            new_data_rows = 8 * self._user_configuration.total_record_size_bytes

            # Can we fit a full set of records into the bank?
            if new_hitmap_rows + hitmap_rows + new_data_rows + data_rows < self.total_rows_for_configurable_data:
                # If we can, add this new block into our existing set
                hitmap_rows += new_hitmap_rows
                data_rows += new_data_rows
                processable_records += self._system_configuration.row_buffer_size_bytes * 8
                continue

            # Can we fit a subset of records into the bank?
            elif new_hitmap_rows + hitmap_rows + data_rows < self.total_rows_for_configurable_data:
                # If we can, ensure we can add at least one data record
                if record_to_row_buffer_ratio >= 1 and (new_hitmap_rows + hitmap_rows +
                   data_rows + record_to_row_buffer_ratio) > self.total_rows_for_configurable_data:
                    # If we can't fit at least one record, break out
                    break

                # At this point at least one record is placeable, so place the blocks
                hitmap_rows += new_hitmap_rows

                rows_remaining = self.total_rows_for_configurable_data - hitmap_rows - data_rows
                if record_to_row_buffer_ratio <= 1:
                    records_per_row = self._system_configuration.row_buffer_size_bytes \
                                      // self._user_configuration.total_record_size_bytes
                    processable_records += rows_remaining * records_per_row
                    data_rows += rows_remaining
                else:
                    records_in_remainder = rows_remaining // (
                            self._user_configuration.total_record_size_bytes
                            // self._system_configuration.row_buffer_size_bytes
                    )
                    processable_records += records_in_remainder
                    data_rows += records_in_remainder * (
                            self._user_configuration.total_record_size_bytes
                            // self._system_configuration.row_buffer_size_bytes
                    )
                break

            # Can no more blocks of rows can be placed
            else:
                break

        # Done heuristically placing rows in bank, finalize configuration

        # Total rows for BLIMP-format records (pi field + data / k + v)
        self.total_rows_for_records = data_rows

        # Total rows for BLIMP hitmap placement
        self.total_rows_for_hitmaps = hitmap_rows

        # Total number of records operable with this configuration
        self.total_records_processable = processable_records

        # Ensure we are inbounds
        if data_rows + hitmap_rows > self.total_rows_for_configurable_data:
            raise AssertionError("Heuristic placement failed, alter parameters or reserved rows")

    def display(self):
        """Dump the configuration into the console for visual inspection"""
        print("System Configuration:")
        self._system_configuration.display()

        print("User Configuration:")
        self._user_configuration.display()

        print("Bank Meta Parameters")
        print(f"    total_rows_for_configurable_data:       {self.total_rows_for_configurable_data}")
        print(f"    total_records_processable:              {self.total_records_processable}")

        print("Bank Row Mappings:")
        print(f"    total_rows_for_blimp_code_region:       {self.total_rows_for_blimp_code_region}")
        print(f"    total_rows_for_records:                 {self.total_rows_for_records}")
        print(f"    total_rows_for_hitmaps:                 {self.total_rows_for_hitmaps}")

    def address_mapping(self):
        """Return the row address mapping for this configuration"""
        base = 0
        mapping = dict()

        # BLIMP Code region always starts at row 0
        mapping["blimp_code_region"] = [base, self.total_rows_for_blimp_code_region]
        base += self.total_rows_for_blimp_code_region

        mapping["records"] = [base, self.total_rows_for_records]
        base += self.total_rows_for_records

        mapping["hitmaps"] = [base, self.total_rows_for_hitmaps]
        base += self.total_rows_for_hitmaps

        return mapping
