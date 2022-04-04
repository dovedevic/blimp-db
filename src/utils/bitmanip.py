def lsb_bit(value: int, least_significant_bit_index: int):
    """Get a bit from the provided value at the specified lsb index (index 0 is lsb)"""
    return value & (1 << least_significant_bit_index) >> least_significant_bit_index


def msb_bit(value: int, most_significant_bit_index: int, bit_width: int):
    """Get a bit from the provided value at the specified msb index (index 0 is msb)"""
    return ((((2**bit_width) - 1) >> most_significant_bit_index) & value) >> (bit_width - most_significant_bit_index - 1)
