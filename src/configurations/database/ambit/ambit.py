from src.configurations.database import DatabaseConfiguration
from src.configurations.database.hitmap import HitmapDatabaseConfiguration


class AmbitDatabaseConfiguration(DatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations"""
    # User-defined Values
    ambit_temporary_bits: int


class AmbitHitmapDatabaseConfiguration(AmbitDatabaseConfiguration, HitmapDatabaseConfiguration):
    """Extends the Ambit database configuration to utilize hitmaps"""
    pass
