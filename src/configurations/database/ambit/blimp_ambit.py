from configurations.database.ambit import AmbitDatabaseConfiguration
from configurations.database.blimp import BlimpDatabaseConfiguration, BlimpVectorDatabaseConfiguration


class BlimpPlusAmbitDatabaseConfiguration(BlimpDatabaseConfiguration, AmbitDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP orchestration"""
    pass


class BlimpVectorPlusAmbitDatabaseConfiguration(BlimpVectorDatabaseConfiguration, AmbitDatabaseConfiguration):
    """Defines changeable AMBIT-only-compute database configurations with BLIMP-V orchestration"""
    pass
