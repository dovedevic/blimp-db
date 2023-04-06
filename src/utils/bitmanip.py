from typing import List


def lsb_bit(value: int, least_significant_bit_index: int) -> int:
    """Get a bit from the provided value at the specified lsb index (index 0 is lsb)"""
    return value & (1 << least_significant_bit_index) >> least_significant_bit_index


def msb_bit(value: int, most_significant_bit_index: int, bit_width: int) -> int:
    """Get a bit from the provided value at the specified msb index (index 0 is msb)"""
    return ((((2**bit_width) - 1) >> most_significant_bit_index) & value) >> (bit_width - most_significant_bit_index - 1)


def int_to_byte_array(value: int, value_byte_size: int) -> List[int]:
    """Given a value, deconstruct it to a byte array with the size specified"""
    byte_array = []
    # Construct a byte array byte-by-byte
    while value > 0:
        byte = value & 0xFF
        value >>= 8
        byte_array.append(byte)
    # 0-pad the value
    byte_array += [0] * (value_byte_size - len(byte_array))
    # Reverse the endianness
    byte_array.reverse()
    return byte_array


def byte_array_to_int(byte_array: [int]) -> int:
    """Given a byte array, construct an integer value representing its bytes"""

    # Ensure this is byte compliant
    if any(byte < 0 or byte >= 256 for byte in byte_array):
        raise ValueError("all values in the byte array must be byte-sized")

    raw_result = 0
    for byte in byte_array:
        raw_result <<= 8
        raw_result += byte
    return raw_result
