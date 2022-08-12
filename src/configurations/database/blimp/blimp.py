from configurations.database.base import DatabaseConfiguration


class BlimpDatabaseConfiguration(DatabaseConfiguration):
    """Defines changeable BLIMP-compute database configurations"""
    # User-defined Values
    hitmap_count: int
    blimp_code_region_size_bytes: int
    blimp_temporary_region_size_bytes: int


class BlimpVectorDatabaseConfiguration(BlimpDatabaseConfiguration):
    """Defines changeable BLIMP-V compute database configurations"""
    pass
