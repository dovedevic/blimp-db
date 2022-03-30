import enum

from src.configuration import SystemConfiguration


class ResultHistoryType(int, enum.Enum):
    CYCLE = 0
    TRA = 1
    COPY = 2


class ResultSet:
    def __init__(self, config: SystemConfiguration):
        self._config = config
        self.history = list()
        self.runtime = 0

    def __add__(self, other):
        if isinstance(other, self.__class__):
            combined_result_set = ResultSet(self._config)
            combined_result_set.history = self.history + other.history
            combined_result_set.runtime = self.runtime + other.runtime
            return combined_result_set
        raise NotImplemented()

    def _add_result(self, r_type: ResultHistoryType, time: float):
        self.history.append((r_type, time))
        self.runtime += time
        return self

    def tra(self):
        return self._add_result(ResultHistoryType.TRA, self._config.time_for_TRA_MAJ)

    def copy(self):
        return self._add_result(ResultHistoryType.COPY, self._config.time_for_AAP_rowclone)

    def cycle(self, cycles=1):
        return self._add_result(ResultHistoryType.CYCLE, self._config.time_per_blimp_cycle * cycles)
