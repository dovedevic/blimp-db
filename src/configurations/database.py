import json

from pydantic import BaseModel


class DatabaseConfiguration(BaseModel):
    """Defines changeable user-case database configurations"""
    # User-defined Values
    total_record_size_bytes: int
    total_index_size_bytes: int

    # Calculated Fields
    total_data_size_bytes: int = None

    def __init__(self, **data):
        super().__init__(**data)
        self.total_data_size_bytes = self.total_record_size_bytes - self.total_index_size_bytes

    def display(self):
        """Dump the configuration into the console for visual inspection"""
        print(json.dumps(self.dict(), indent=4))

    def save(self, path: str, compact=False):
        """
        Save the database configuration object as a JSON object

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
        """Load a database configuration object"""
        with open(path, 'r') as fp:
            return cls(**json.load(fp))


class BlimpDatabaseConfiguration(DatabaseConfiguration):
    """Defines changeable BLIMP-compute database configurations"""
    # User-defined Values
    hitmap_count: int
    blimp_code_region_size_bytes: int
    temporary_bytes: int


class BlimpVectorDatabaseConfiguration(BlimpDatabaseConfiguration):
    """Defines changeable BLIMP-V compute database configurations"""
    pass


class AmbitDatabaseConfiguration(DatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations"""
    # User-defined Values
    ambit_temporary_bits: int


class BlimpPlusAmbitDatabaseConfiguration(BlimpDatabaseConfiguration, AmbitDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP orchestration"""
    pass


class BlimpVectorPlusAmbitDatabaseConfiguration(BlimpVectorDatabaseConfiguration, AmbitDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP-V orchestration"""
    pass
