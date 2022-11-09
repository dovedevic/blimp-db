import typing


class DataGenerator:
    """Base Class for generic data generation"""
    def __init__(self, data_size_in_bytes: int, pregenerated_items: int=0, generatable: bool=True):

        assert data_size_in_bytes >= 0, "Data sizes must be positive integers"
        assert pregenerated_items >= 0, "Item counts must be positive integers"

        self._data_size_in_bytes = data_size_in_bytes
        self._items_generated = pregenerated_items
        self._generatable = generatable

    def generate_data(self) -> typing.Optional[int]:
        """
        @final no override
        public accessor for generating data; handles metadata updating
        """
        if not self._generatable:
            raise RuntimeError("This generator is not able to generate more records")

        data = 0 if self._data_size_in_bytes <= 0 else self._generate()
        self._items_generated += 1
        return data

    @property
    def items_generated(self):
        """The amount of records generated in this instance"""
        return self._items_generated

    @property
    def data_size(self):
        """The length or amount of data per generation, in bytes"""
        return self._data_size_in_bytes

    @property
    def generatable(self):
        """The state of the generation production capability"""
        return self._generatable

    def set_generatable(self, state: bool):
        """Set the state of the generation production capability"""
        self._generatable = state

    def _generate(self) -> typing.Optional[int]:
        """
        @implementable
        Overridable method for generating data
        """
        return 0
