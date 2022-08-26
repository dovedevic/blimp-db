from configurations.database.ambit import AmbitDatabaseConfiguration, AmbitHitmapDatabaseConfiguration
from configurations.database.blimp import BlimpDatabaseConfiguration, BlimpVectorDatabaseConfiguration, \
    BlimpHitmapDatabaseConfiguration


class BlimpPlusAmbitDatabaseConfiguration(BlimpDatabaseConfiguration, AmbitDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP orchestration"""
    pass


class BlimpVectorPlusAmbitDatabaseConfiguration(BlimpVectorDatabaseConfiguration, AmbitDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP-V orchestration"""
    pass


class BlimpPlusAmbitHitmapDatabaseConfiguration(BlimpHitmapDatabaseConfiguration, AmbitHitmapDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP orchestration with hitmaps"""
    pass
