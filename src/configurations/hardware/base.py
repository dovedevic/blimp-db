import json

from pydantic import BaseModel


class HardwareConfiguration(BaseModel):
    """Defines unchanging system configurations intrinsic to the hardware placed in a system at runtime"""
    # Intrinsic Hardware Values
    bank_size_bytes: int
    row_buffer_size_bytes: int
    time_to_row_activate_ns: float
    time_to_column_activate_ns: float
    time_to_precharge_ns: float
    time_to_bank_communicate_ns: float
    cpu_frequency: int

    # Calculated Fields
    bank_rows: int = None

    def __init__(self, **data):
        super().__init__(**data)
        self.bank_rows = self.bank_size_bytes // self.row_buffer_size_bytes

    def display(self):
        """Dump the configuration into the console for visual inspection"""
        print(json.dumps(self.dict(), indent=4))

    def save(self, path: str, compact=False):
        """
        Save the system configuration object as a JSON object

        @param path: The path and filename to save the configuration
        @param compact: Whether the JSON saved should be compact or indented
        """
        with open(path, "w") as fp:
            if compact:
                json.dump(self.dict(), fp)
            else:
                json.dump(self.dict(), fp, indent=4)

    @classmethod
    def load(cls, path: str):
        """Load a system configuration object"""
        with open(path, 'r') as fp:
            return cls(**json.load(fp))
