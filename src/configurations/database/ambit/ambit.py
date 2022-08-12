from configurations.database import DatabaseConfiguration


class AmbitDatabaseConfiguration(DatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations"""
    # User-defined Values
    ambit_temporary_bits: int
