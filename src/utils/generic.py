import math


def round_to_multiple(x, base):
    return base * round(x/base)


def ceil_to_multiple(x, base):
    return base * math.ceil(x/base)
