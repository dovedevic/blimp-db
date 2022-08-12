from generators import DataGenerator


class NullDataGenerator(DataGenerator):
    """Generate null data"""
    def __init__(self):
        super().__init__(0)

    def _generate(self):
        return None
