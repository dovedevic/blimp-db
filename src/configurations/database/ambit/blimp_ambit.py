from src.configurations.database.ambit import AmbitDatabaseConfiguration
from src.configurations.database.blimp import BlimpDatabaseConfiguration, BlimpVectorDatabaseConfiguration
from src.configurations.database.hitmap import HitmapDatabaseConfiguration


class BlimpPlusAmbitDatabaseConfiguration(BlimpDatabaseConfiguration, AmbitDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP orchestration"""
    pass


class BlimpVectorPlusAmbitDatabaseConfiguration(BlimpVectorDatabaseConfiguration, AmbitDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP-V orchestration"""
    pass


class BlimpPlusAmbitHitmapDatabaseConfiguration(BlimpDatabaseConfiguration,
                                                AmbitDatabaseConfiguration,
                                                HitmapDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP orchestration with hitmaps"""
    pass
