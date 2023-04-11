from src.configurations.database.base import DatabaseConfiguration
from src.configurations.database.hitmap import HitmapDatabaseConfiguration


class BlimpDatabaseConfiguration(DatabaseConfiguration):
    """Defines changeable BLIMP-compute database configurations"""
    # User-defined Values
    blimp_code_region_size_bytes: int
    blimp_temporary_region_size_bytes: int


class BlimpVectorDatabaseConfiguration(BlimpDatabaseConfiguration):
    """Defines changeable BLIMP-V compute database configurations"""
    pass


class BlimpHitmapDatabaseConfiguration(BlimpDatabaseConfiguration, HitmapDatabaseConfiguration):
    """Extends the BLIMP database configuration to utilize hitmaps"""
    pass


class BlimpVectorHitmapDatabaseConfiguration(BlimpHitmapDatabaseConfiguration):
    """Extends the BLIMP-V database configuration to utilize hitmaps"""
    pass
