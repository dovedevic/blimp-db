from configurations.database import DatabaseConfiguration


class AmbitDatabaseConfiguration(DatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations"""
    # User-defined Values
    ambit_temporary_bits: int


class AmbitHitmapDatabaseConfiguration(AmbitDatabaseConfiguration):
    """Extends the Ambit database configuration to utilize hitmaps"""
    hitmap_count: int
