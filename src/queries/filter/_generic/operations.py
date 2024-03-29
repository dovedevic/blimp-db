from enum import Enum


class GenericArithmeticLogicalOperation(str, Enum):
    LT = "<"
    GT = ">"
    EQ = "="
    NEQ = "!="
    LTE = "<="
    GTE = ">="
