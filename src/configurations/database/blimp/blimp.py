from configurations.database.base import DatabaseConfiguration


class BlimpDatabaseConfiguration(DatabaseConfiguration):
    """Defines changeable BLIMP-compute database configurations"""
    # User-defined Values
    blimp_code_region_size_bytes: int
    blimp_temporary_region_size_bytes: int


class BlimpVectorDatabaseConfiguration(BlimpDatabaseConfiguration):
    """Defines changeable BLIMP-V compute database configurations"""
    pass


class BlimpHitmapDatabaseConfiguration(BlimpDatabaseConfiguration):
    """Extends the BLIMP database configuration to utilize hitmaps"""
    hitmap_count: int


class BlimpVectorHitmapDatabaseConfiguration(BlimpHitmapDatabaseConfiguration):
    """Extends the BLIMP-V database configuration to utilize hitmaps"""
    pass
