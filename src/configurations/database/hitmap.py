from src.configurations.database import DatabaseConfiguration


class HitmapDatabaseConfiguration(DatabaseConfiguration):
    """Extends existing database configurations to utilize hitmaps"""
    hitmap_count: int
