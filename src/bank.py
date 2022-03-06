import logging


class Bank:
    def __init__(self, bank_size, row_buffer_size, bank=None):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info("creating bank with prebuilt rows..." if bank else "creating bank with null rows")

        self.bank_size = bank_size
        self.row_buffer_size = row_buffer_size
        self.bank = bank or [
            [0] * (row_buffer_size * 8)
        ] * (bank_size // row_buffer_size)

        self.logger.info(f"bank stats -- size: {self.bank_size} rb_size: {self.row_buffer_size} bank_rows: {len(self.bank)}")

    def save(self, name, prefix='banks'):
        self.logger.info(f"saving bank configuration as: " + f"{prefix}/{name}.{self.bank_size}.{self.row_buffer_size}.bank")
        with open(f"{prefix}/{name}.{self.bank_size}.{self.row_buffer_size}.bank", "w") as fp:
            for row in self.bank:
                fp.write(" ".join([str(b) for b in row]))
                fp.write("\n")

    @staticmethod
    def load(name, prefix='banks'):
        with open(f"{prefix}/{name}", "r") as fp:
            bank = []
            rb_size = 0
            for row in fp.readlines():
                row = [int(b) for b in row.split(' ')]
                rb_size = len(row)
                bank.append(row)
            bank_size = len(bank) * rb_size
        return Bank(bank_size, rb_size, bank)

    def set_row(self, row_index, value: list):
        self.bank[row_index] = value

    def set_raw_row(self, row_index, value):
        bits = [int(b) for b in str(bin(value))[2:]][:self.row_buffer_size * 8]
        bits = [0] * (self.row_buffer_size * 8 - len(bits)) + bits
        self.bank[row_index] = bits

    def set_ones(self, row_index):
        self.set_raw_row(row_index, int.from_bytes(b'\xFF' * self.row_buffer_size, "big"))

    def set_zeros(self, row_index):
        self.set_raw_row(row_index, 0)

    def get_row(self, row_index, invert=False):
        result = self.bank[row_index]
        if invert:
            result = [int(not b) for b in result]
        return result

    def copy_row(self, from_index, to_index, invert=False):
        result = self.get_row(from_index, invert)
        self.set_row(to_index, result)
        return result

    def invert_row(self, index):
        result = self.get_row(index, invert=True)
        self.set_row(index, result)
        return result

    def maj_rows(self, mag_index_a, mag_index_b, mag_index_c, protect_a=False, protect_b=False, protect_c=False, invert=False):
        mag_a = self.get_row(mag_index_a)
        mag_b = self.get_row(mag_index_b)
        mag_c = self.get_row(mag_index_c)
        result = []

        for bit in range(self.row_buffer_size * 8):
            result.append(int((mag_a[bit] + mag_b[bit] + mag_c[bit]) > 1))

        if invert:
            result = [int(not b) for b in result]

        if not protect_a:
            self.set_row(mag_index_a, result)
        if not protect_b:
            self.set_row(mag_index_b, result)
        if not protect_c:
            self.set_row(mag_index_c, result)

        return result
